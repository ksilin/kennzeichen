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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static com.example.KennzeichenSerdes.*;
import static com.example.KennzeichenTopologyNames.PER_PLATE_STORE;
import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
class CarStateChangePunctuatorTest {

    static String inputTopicName = "carCameraEvents";
    static String outputTopicName = "carEventNotifications";

    static final Properties props = new Properties();

    @BeforeAll
    static void beforeAll() {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "car-camera-stream-processor-v0.47");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
    }

    @Test
    void punctuatorMustPushCarChangeEventAfterTimeoutTest() {

        Topology topology = makeTestTopology();
        TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);
        TestOutputTopic<String, CarStateChanged> outputTopic = testDriver.createOutputTopic(outputTopicName, STRING_SERDE.deserializer(), CAR_STATE_CHANGED_SERDE.deserializer());
        KeyValueStore<String, CarCamEventAggregation> perPlateStore = testDriver.getKeyValueStore(PER_PLATE_STORE);

        long now = Instant.now().toEpochMilli();

        String carID = "123";
        String plateCompleteSession = "ABCDEF";
        var eventCompleteSession = CarCamEventBuilder.CarCamEvent(carID, "new", plateCompleteSession, "DEU", now, "front", "FFF", 0.6f, "out");

        perPlateStore.put(plateCompleteSession, CarCamEventAggregation.from(List.of(eventCompleteSession)));

        String plateOngoingSession = "XYZ";

        var eventOngoingSession = eventCompleteSession.withCaptureTimestamp(now + 10000).withPlateUTF8(plateOngoingSession);
        perPlateStore.put(plateOngoingSession, CarCamEventAggregation.from(List.of(eventOngoingSession)));

        // trigger punctuator
        testDriver.advanceWallClockTime(Duration.ofSeconds(10));

        // output topic contains state change event for completed session
        var carStateChangedEvents = outputTopic.readKeyValuesToList();
        assertThat(carStateChangedEvents.size()).isEqualTo(1);
        assertThat(carStateChangedEvents.getFirst().key).isEqualTo(plateCompleteSession);

        // completed session has been removed from state store
        assertThat(perPlateStore.get(plateCompleteSession)).isNull();

        // ongoing session is still in state store
        assertThat(perPlateStore.get(plateOngoingSession).events().get(0).plateUTF8()).isEqualTo(plateOngoingSession);

        testDriver.close();
    }

    Topology makeTestTopology() {

        var storeBuilder = KennzeichenTopologyProducer.makePerPlateStore();

        var builder = new StreamsBuilder();
        builder.addStateStore(storeBuilder);
        ProcessorSupplier<String, CarCamEvent, String, CarStateChanged> processorSupplier = () -> new CarStatusChangedPunctuateProcessor(1000L);
        KStream<String, CarCamEvent> stream = builder.stream(inputTopicName, Consumed.with(Serdes.String(), CAR_CAM_EVENT_SERDE));
        stream
                .process(processorSupplier, Named.as("carStateChangedPunctuator"), PER_PLATE_STORE)
                .to(outputTopicName, Produced.with(Serdes.String(), CAR_STATE_CHANGED_SERDE));

        return builder.build();
    }


}
