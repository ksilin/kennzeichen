package com.example;

import io.quarkus.test.junit.QuarkusTest;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
class CarStatusChangedPunctuateTest {

    static String inputTopicName = "carCameraEvents";
    static String outputTopicName = "carEventNotifications";

    static final Properties props = new Properties();

    Logger log = Logger.getLogger(CarStatusChangedPunctuateTest.class);

    @BeforeAll
    static void beforeAll() {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "car-camera-stream-processor-v0.47");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
    }

    @Test
    void punctuatorMustPushCarChangeEventAfterTimeoutTest() {
        Topology topology = makeTestTopology();

        var testDriver = new TopologyTestDriver(topology, props);

        TestOutputTopic<String, CarStateChanged> outputTopic = testDriver.createOutputTopic(outputTopicName, CarCamEventTopologyProducer.stringSerde.deserializer(), CarCamEventTopologyProducer.carStateChangedSerde.deserializer());

         KeyValueStore<String, CarCamEventAggregation> perPlateStore = testDriver.getKeyValueStore(CarCamEventTopologyProducer.PER_PLATE_STORE);

        long now = Instant.now().toEpochMilli();

        String carID = "123";
        String plate1 = "ABCDEF";
        var eventEarly = CarCamEventBuilder.CarCamEvent(carID, "new", plate1, "DEU", now, "front", "FFF", 0.6f, "out");

        perPlateStore.put(plate1, CarCamEventAggregation.from(List.of(eventEarly)));

        String plate2 = "XYZ";

        var eventLate = CarCamEventBuilder.CarCamEvent(carID, "new", plate2, "DEU", now + 10000, "front", "FFF", 0.6f, "out");
        perPlateStore.put(plate2, CarCamEventAggregation.from(List.of(eventLate)));


        testDriver.advanceWallClockTime(Duration.ofSeconds(10));

        var kvs = outputTopic.readKeyValuesToList();

        assertThat(kvs.size()).isEqualTo(1);
        assertThat(kvs.get(0).key).isEqualTo(plate1);
        assertThat(perPlateStore.get(plate1)).isNull();
        assertThat(perPlateStore.get(plate2).events().get(0).plateUTF8()).isEqualTo(plate2);

        log.infov("received {0} events: ", kvs.size());
        kvs.forEach(k -> log.info(k.toString()));

        testDriver.close();
    }

    Topology makeTestTopology(){

        var storeBuilder = CarCamEventTopologyProducer.makePerPlateStore();

        var builder =  new StreamsBuilder();
        builder.addStateStore(storeBuilder);
        ProcessorSupplier<String, CarCamEvent, String, CarStateChanged> processorSupplier =  () -> new CarStatusChangedPunctuateProcessor(1000L);
        KStream<String, CarCamEvent> stream = builder.stream(inputTopicName, Consumed.with(Serdes.String(), CarCamEventTopologyProducer.carCameraEventSerde));
        stream
                .process(processorSupplier, Named.as("carStateChangedPunctuator"), CarCamEventTopologyProducer.PER_PLATE_STORE)
                .to(outputTopicName, Produced.with(Serdes.String(), CarCamEventTopologyProducer.CarStateChangedSerde));

      return builder.build();
    };


}
