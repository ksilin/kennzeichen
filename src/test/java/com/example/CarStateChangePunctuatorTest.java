package com.example;

import com.example.model.*;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static com.example.KennzeichenSerdes.*;
import static com.example.KennzeichenTopologyNames.PER_PLATE_STORE_NAME;
import static com.example.model.CarCamEvent.STATE_NEW;
import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
class CarStateChangePunctuatorTest {

    static String inputTopicName = "carCameraEvents";
    static String outputTopicName = "carEventNotifications";

    PodamFactory mockPojoFactory = new PodamFactoryImpl();

    long eventTimeoutThreshold = 1000L;
    static final Properties props = new Properties();

    @BeforeAll
    static void beforeAll() {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "car-camera-stream-processor-v0.47");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
    }

    @Test
    void mustPushCarChangeEventAndRemoveAggregationAfterTimeout() {

        Instant instantNow = Instant.now();
        long now = instantNow.toEpochMilli();

        String plate = "ABCDEF";
        var event1 = mockPojoFactory.manufacturePojoWithFullData(CarCamEvent.class).withCarState(STATE_NEW).withPlateUTF8(plate).withPlateConfidence(0.8f);
        var event2 = event1.withCaptureTimestamp(now + eventTimeoutThreshold + 1);

        try(TopologyTestDriver testDriver = new TopologyTestDriver(makeTestTopology(eventTimeoutThreshold), props, instantNow)) {
            TestOutputTopic<String, CarStateChanged> outputTopic = testDriver.createOutputTopic(outputTopicName, STRING_SERDE.deserializer(), CAR_STATE_CHANGED_SERDE.deserializer());
            KeyValueStore<String, CarCamEventAggregation> perPlateStore = testDriver.getKeyValueStore(PER_PLATE_STORE_NAME);

            perPlateStore.put(plate, CarCamEventAggregation.from(List.of(event1, event2)));

            // trigger punctuator
            testDriver.advanceWallClockTime(Duration.ofSeconds(10));

            // output topic contains no state change events
            var carStateChangedEvents = outputTopic.readKeyValuesToList();
            assertThat(carStateChangedEvents).isEmpty();

            // ongoing session is still in state store
            CarCamEventAggregation aggregation = perPlateStore.get(plate);
            assertThat(aggregation).isNotNull();
            assertThat(aggregation.events().get(0)).isEqualTo(event1);
            assertThat(aggregation.events().get(1)).isEqualTo(event2);
        }
    }

    @Test
    void mustNotPushEventAndRetainAggregationBeforeTimeout() {

        Instant instantNow = Instant.now();
        long now = instantNow.toEpochMilli();
        String plate = "ABCDEF";
        var event1 = mockPojoFactory.manufacturePojoWithFullData(CarCamEvent.class).withCaptureTimestamp(now).withCarState(STATE_NEW).withPlateUTF8(plate).withPlateConfidence(0.8f);
        var event2 = event1.withCaptureTimestamp(now + eventTimeoutThreshold - 1);

        try(TopologyTestDriver testDriver = new TopologyTestDriver(makeTestTopology(eventTimeoutThreshold), props, instantNow)) {
            TestOutputTopic<String, CarStateChanged> outputTopic = testDriver.createOutputTopic(outputTopicName, STRING_SERDE.deserializer(), CAR_STATE_CHANGED_SERDE.deserializer());
            KeyValueStore<String, CarCamEventAggregation> perPlateStore = testDriver.getKeyValueStore(PER_PLATE_STORE_NAME);

            perPlateStore.put(plate, CarCamEventAggregation.from(List.of(event1, event2)));

            // trigger punctuator
            testDriver.advanceWallClockTime(Duration.ofSeconds(10));

            // output topic contains state change event for completed session
            var carStateChangedEvents = outputTopic.readKeyValuesToList();
            assertThat(carStateChangedEvents.size()).isEqualTo(1);
            assertThat(carStateChangedEvents.getFirst().key).isEqualTo(plate);

            // completed session has been removed from state store
            assertThat(perPlateStore.get(plate)).isNull();
        }
    }

    Topology makeTestTopology(long eventTimeoutThreshold) {

        var storeBuilder = KennzeichenTopologyProducer.makePerPlateStore();

        var builder = new StreamsBuilder();
        builder.addStateStore(storeBuilder);

        ProcessorSupplier<String, CarCamEvent, String, CarStateChanged> processorSupplier = () -> new CarStateChangedPunctuateProcessor(eventTimeoutThreshold);
        KStream<String, CarCamEvent> stream = builder.stream(inputTopicName, Consumed.with(Serdes.String(), CAR_CAM_EVENT_SERDE));
        stream
                .process(processorSupplier, Named.as("carStateChangedPunctuator"), PER_PLATE_STORE_NAME)
                .to(outputTopicName, Produced.with(Serdes.String(), CAR_STATE_CHANGED_SERDE));

        return builder.build();
    }

}
