package com.example;

import com.example.model.CarCamEvent;
import com.example.model.CarCamEventAggregation;
import com.example.model.CarStateChanged;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

import java.util.List;
import java.util.Properties;

import static com.example.KennzeichenSerdes.*;
import static com.example.KennzeichenTopologyNames.PER_PLATE_STORE_NAME;
import static com.example.model.CarCamEvent.STATE_NEW;
import static com.example.model.CarCamEvent.STATE_UPDATE;
import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
class CarCamEventProcessorTest {

    static String inputTopicName = "carCameraEvents";
    static String outputTopicName = "carEventNotifications";

    static final Properties props = new Properties();

    Logger log = Logger.getLogger(CarCamEventProcessorTest.class);

    PodamFactory mockPojoFactory = new PodamFactoryImpl();

    @BeforeAll
    static void beforeAll() {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "car-camera-stream-processor-v0.47");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
    }

    // more tests for:
    // * similar plates being grouped together

    @Test
    void mustGroupSimilarEventsByPlate() {

        String key = "AAA";
        String plate1 = "ABCDEF";
        var event1 = mockPojoFactory.manufacturePojoWithFullData(CarCamEvent.class).withCarState(STATE_NEW).withPlateUTF8(plate1).withPlateConfidence(0.8f);

        String plate2 = "ABCDEFG";
        var event2 = event1.withPlateUTF8(plate2).withCarState(STATE_UPDATE);

        try (TopologyTestDriver testDriver = new TopologyTestDriver(makeTestTopology(), props)) {

            TestInputTopic<String, CarCamEvent> inputTopic = testDriver.createInputTopic(inputTopicName, STRING_SERDE.serializer(), CAR_CAM_EVENT_SERDE.serializer());
            TestOutputTopic<String, CarStateChanged> outputTopic = testDriver.createOutputTopic(outputTopicName, STRING_SERDE.deserializer(), CAR_STATE_CHANGED_SERDE.deserializer());
            KeyValueStore<String, CarCamEventAggregation> perPlateStore = testDriver.getKeyValueStore(PER_PLATE_STORE_NAME);

            inputTopic.pipeInput(key, event1);
            inputTopic.pipeInput(key, event2);

            // no vents are expected in the output topic
            var carStateChangedEvents = outputTopic.readKeyValuesToList();
            assertThat(carStateChangedEvents.size()).isEqualTo(0);

            // completed session has been removed from state store
            CarCamEventAggregation aggregation1 = perPlateStore.get(plate1);
            CarCamEventAggregation aggregation2 = perPlateStore.get(plate2);

            assertThat(aggregation1).isNotNull();
            assertThat(aggregation2).isNull();

            assertThat(aggregation1.events()).hasSize(2);

            // ongoing session is still in state store
            assertThat(aggregation1.events().get(0).plateUTF8()).isEqualTo(plate1);
            assertThat(aggregation1.events().get(1).plateUTF8()).isEqualTo(plate2);
        }
    }

    @Test
    void mustCollectAllNewEventsByPlate() {

        String key = "AAA";
        String plate1 = "ABCDEF";
        var event1 = mockPojoFactory.manufacturePojoWithFullData(CarCamEvent.class).withCarState(STATE_NEW).withPlateUTF8(plate1).withPlateConfidence(0.8f);

        String plate2 = "UVXYZ";
        var event2 = event1.withPlateUTF8(plate2);

        try (TopologyTestDriver testDriver = new TopologyTestDriver(makeTestTopology(), props)) {
            TestInputTopic<String, CarCamEvent> inputTopic = testDriver.createInputTopic(inputTopicName, STRING_SERDE.serializer(), CAR_CAM_EVENT_SERDE.serializer());
            TestOutputTopic<String, CarStateChanged> outputTopic = testDriver.createOutputTopic(outputTopicName, STRING_SERDE.deserializer(), CAR_STATE_CHANGED_SERDE.deserializer());
            KeyValueStore<String, CarCamEventAggregation> perPlateStore = testDriver.getKeyValueStore(PER_PLATE_STORE_NAME);

            inputTopic.pipeInput(key, event1);
            inputTopic.pipeInput(key, event2);

            // no vents are expected in the output topic
            var carStateChangedEvents = outputTopic.readKeyValuesToList();
            assertThat(carStateChangedEvents.size()).isEqualTo(0);

            // completed session has been removed from state store
            CarCamEventAggregation aggregation1 = perPlateStore.get(plate1);
            CarCamEventAggregation aggregation2 = perPlateStore.get(plate2);

            assertThat(aggregation1).isNotNull();
            assertThat(aggregation2).isNotNull();

            assertThat(aggregation1.events()).hasSize(1);
            assertThat(aggregation2.events()).hasSize(1);

            // ongoing session is still in state store
            assertThat(aggregation1.events().get(0).plateUTF8()).isEqualTo(plate1);
            assertThat(aggregation2.events().get(0).plateUTF8()).isEqualTo(plate2);
        }
    }

    @Test
    void mustAggregateUpdateEventsByPlateIfAggregationExists() {

        String key = "AAA";
        String plate1 = "ABCDEF";
        var event1 = mockPojoFactory.manufacturePojoWithFullData(CarCamEvent.class).withCarState(STATE_NEW).withPlateUTF8(plate1).withPlateConfidence(0.8f);

        String plate2 = "UVXYZ";
        var event2 = event1.withPlateUTF8(plate2);

        try (TopologyTestDriver testDriver = new TopologyTestDriver(makeTestTopology(), props)) {
            TestInputTopic<String, CarCamEvent> inputTopic = testDriver.createInputTopic(inputTopicName, STRING_SERDE.serializer(), CAR_CAM_EVENT_SERDE.serializer());
            TestOutputTopic<String, CarStateChanged> outputTopic = testDriver.createOutputTopic(outputTopicName, STRING_SERDE.deserializer(), CAR_STATE_CHANGED_SERDE.deserializer());
            KeyValueStore<String, CarCamEventAggregation> perPlateStore = testDriver.getKeyValueStore(PER_PLATE_STORE_NAME);

            // preparing the state store
            perPlateStore.put(plate1, CarCamEventAggregation.from(List.of(event1)));
            perPlateStore.put(plate2, CarCamEventAggregation.from(List.of(event2)));

            inputTopic.pipeInput(key, event1);
            inputTopic.pipeInput(key, event2);

            // no vents are expected in the output topic
            var carStateChangedEvents = outputTopic.readKeyValuesToList();
            assertThat(carStateChangedEvents.size()).isEqualTo(0);

            // completed session has been removed from state store
            CarCamEventAggregation aggregation1 = perPlateStore.get(plate1);
            CarCamEventAggregation aggregation2 = perPlateStore.get(plate2);

            assertThat(aggregation1).isNotNull();
            assertThat(aggregation2).isNotNull();

            assertThat(aggregation1.events()).hasSize(2);
            assertThat(aggregation2.events()).hasSize(2);

            // ongoing session is still in state store
            assertThat(aggregation1.events().get(0).plateUTF8()).isEqualTo(plate1);
            assertThat(aggregation2.events().get(0).plateUTF8()).isEqualTo(plate2);
        }
    }

    @Test
    void mustIgnoreAllUpdateEventsIfNoAggregationExists() {

        String key = "AAA";
        String plate1 = "ABCDEF";
        var event1 = mockPojoFactory.manufacturePojoWithFullData(CarCamEvent.class).withCarState(STATE_UPDATE).withPlateUTF8(plate1).withPlateConfidence(0.8f);

        String plate2 = "UVXYZ";
        var event2 = event1.withPlateUTF8(plate2);

        try (TopologyTestDriver testDriver = new TopologyTestDriver(makeTestTopology(), props)) {
            TestInputTopic<String, CarCamEvent> inputTopic = testDriver.createInputTopic(inputTopicName, STRING_SERDE.serializer(), CAR_CAM_EVENT_SERDE.serializer());
            KeyValueStore<String, CarCamEventAggregation> perPlateStore = testDriver.getKeyValueStore(PER_PLATE_STORE_NAME);

            inputTopic.pipeInput(key, event1);
            inputTopic.pipeInput(key, event2);

            // completed session has been removed from state store
            CarCamEventAggregation aggregation1 = perPlateStore.get(plate1);
            CarCamEventAggregation aggregation2 = perPlateStore.get(plate2);

            assertThat(aggregation1).isNull();
            assertThat(aggregation2).isNull();
        }
    }

    Topology makeTestTopology() {

        var storeBuilder = KennzeichenTopologyProducer.makePerPlateStore();

        var builder = new StreamsBuilder();
        builder.addStateStore(storeBuilder);
        ProcessorSupplier<String, CarCamEvent, String, CarStateChanged> processorSupplier = CarCamEventProcessor::new;
        KStream<String, CarCamEvent> stream = builder.stream(inputTopicName, Consumed.with(Serdes.String(), CAR_CAM_EVENT_SERDE));
        stream
                .process(processorSupplier, Named.as(KennzeichenTopologyNames.CAR_CAM_EVENT_PROCESSOR_NAME), PER_PLATE_STORE_NAME)
                .to(outputTopicName, Produced.with(Serdes.String(), CAR_STATE_CHANGED_SERDE));
        return builder.build();
    }


}
