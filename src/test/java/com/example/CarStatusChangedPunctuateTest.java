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
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

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
    void happyCaseOuterSensorsFirstThenInnerSensors(){
        // {"ts":1701437161585,"state":"new","plate":"XXYYZZ","cam":"einfahrt_front","dir":"unknown"}
        // {"ts":1701437161705,"state":"update","plate":"XXYYZZ","cam":"einfahrt_front","dir":"in"}
        // {"ts":1701437173126,"state":"new","plate":"XXYYZZ","cam":"einfahrt_heck","dir":"unknown"}
        // {"ts":1701437173166,"state":"update","plate":"XXYYZZ","cam":"einfahrt_heck","dir":"in"}

        var now = Instant.now().toEpochMilli();

        var eventFrontUnknownNew = CarCamEventBuilder.CarCamEvent("1", "new", "ABCDEF", "DEU", now, "einfahrt_front", "GAZ", 0.7f, "unknown");
        var eventFrontInUpdate = eventFrontUnknownNew.withCarMoveDirection("in").withCarState("update").withCaptureTimestamp(now + 1000);
        var eventHeckUnknownNew = eventFrontUnknownNew.withSensorProviderID("einfahrt_heck").withCaptureTimestamp(now + 10000);
        var eventHeckInUpdate = eventFrontUnknownNew.withCarMoveDirection("in").withCarState("update").withSensorProviderID("einfahrt_heck").withCaptureTimestamp(now + 11000);

       CarCamEventAggregation agg = CarCamEventAggregationBuilder.CarCamEventAggregation(List.of(eventFrontUnknownNew, eventFrontInUpdate, eventHeckUnknownNew, eventHeckInUpdate));

        var carStateChange = carStateFromAggregation(agg);

        assertThat(carStateChange.newState()).isEqualTo("EINFAHRT");
    }


    private CarStateChanged carStateFromAggregation(CarCamEventAggregation agg){

        var events = agg.events();

        var directions = events.stream().map(CarCamEvent::carMoveDirection).filter(e -> !e.equals("unknown")).collect(Collectors.toUnmodifiableList());

        String state = "UNKNOWN";
        if(!directions.isEmpty()){
            state = directions.getFirst().equals("in")? "EINFAHRT" : "AUSFAHRT";
        }

        // TODO - count by group
        return CarStateChangedBuilder.CarStateChanged(agg.events().get(0).plateUTF8(), state);
    }


    @Test
    void punctuatorMustPushCarChangeEventAfterTimeoutTest() {
        Topology topology = makeTestTopology();

        var testDriver = new TopologyTestDriver(topology, props);

        TestOutputTopic<String, CarStateChanged> outputTopic = testDriver.createOutputTopic(outputTopicName, CarCamEventTopologyProducer.stringSerde.deserializer(), CarCamEventTopologyProducer.carStateChangedSerde.deserializer());

         KeyValueStore<String, CarCamEventAggregation> perPlateStore = testDriver.getKeyValueStore(CarCamEventTopologyProducer.PER_PLATE_STORE);

        long now = Instant.now().toEpochMilli();

        String carID = "123";
        String plateCompleteSession = "ABCDEF";
        var eventEarly = CarCamEventBuilder.CarCamEvent(carID, "new", plateCompleteSession, "DEU", now, "front", "FFF", 0.6f, "out");

        perPlateStore.put(plateCompleteSession, CarCamEventAggregation.from(List.of(eventEarly)));

        String plateOngoingSession = "XYZ";

        var eventLate = CarCamEventBuilder.CarCamEvent(carID, "new", plateOngoingSession, "DEU", now + 10000, "front", "FFF", 0.6f, "out");
        perPlateStore.put(plateOngoingSession, CarCamEventAggregation.from(List.of(eventLate)));

        // trigger punctuator
        testDriver.advanceWallClockTime(Duration.ofSeconds(10));

        var kvs = outputTopic.readKeyValuesToList();

        assertThat(kvs.size()).isEqualTo(1);
        assertThat(kvs.getFirst().key).isEqualTo(plateCompleteSession);
        assertThat(perPlateStore.get(plateCompleteSession)).isNull();
        assertThat(perPlateStore.get(plateOngoingSession).events().get(0).plateUTF8()).isEqualTo(plateOngoingSession);

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
    }


}
