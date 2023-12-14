package com.example;

import com.example.model.*;
import io.quarkus.test.junit.QuarkusTest;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

import static com.example.KennzeichenSerdes.*;

@QuarkusTest
class CarCamEventTopologyTest {

    static String inputTopicName = "carCameraEvents";
    static String outputTopicName = "carEventNotifications";

    static final Properties props = new Properties();

    Logger log = Logger.getLogger(CarCamEventTopologyTest.class);

    @BeforeAll
    static void beforeAll() {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "car-camera-stream-processor-v0.47");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
    }

    //@Test
    void mustFindMostSimilarPlates() {

        List<String> plateChanges = List.of("ABCDEG", "BCDEF", "BCDE", "FEDCBA", "BBCDEG");

        Topology topo = KennzeichenTopologyProducer.createTopology(inputTopicName, outputTopicName);

        TopologyTestDriver testDriver = new TopologyTestDriver(topo, props);
        TestInputTopic<String, CarCamEvent> inputTopic = testDriver.createInputTopic(inputTopicName, STRING_SERDE.serializer(), CAR_CAM_EVENT_SERDE.serializer());
        TestOutputTopic<String, CarStateChanged> outputTopic = testDriver.createOutputTopic(outputTopicName, STRING_SERDE.deserializer(), CAR_STATE_CHANGED_SERDE.deserializer());


        String carID = "123";
        String plate = "ABCDEF";
        long now = Instant.EPOCH.toEpochMilli();
        var event = CarCamEventBuilder.CarCamEvent(carID, "new", plate, "DEU", now, "front", "FFF", 0.6f, "out");

        inputTopic.pipeInput("hi", event, now);

        IntStream.range(0, plateChanges.size())
                .forEach(index -> {
                             inputTopic.pipeInput("hi", event.withCarState("update").withPlateUTF8(plateChanges.get(index)), now + 1000 * index);
                             testDriver.advanceWallClockTime(Duration.ofSeconds(1));
                         }
                );

        log.info("resulting events:");
        var kvs = outputTopic.readKeyValuesToList();
        kvs.forEach(k -> log.info(k.toString()));

        testDriver.close();
    }

    //@Test
    void plateCarIdDivergenceTest() {
        Topology topology = KennzeichenTopologyProducer.createTopology(inputTopicName, outputTopicName);

        var testDriver = new TopologyTestDriver(topology, props);

        TestInputTopic<String, CarCamEvent> inputTopic = testDriver.createInputTopic(inputTopicName, STRING_SERDE.serializer(), CAR_CAM_EVENT_SERDE.serializer());

        TestOutputTopic<String, CarStateChanged> outputTopic = testDriver.createOutputTopic(outputTopicName, STRING_SERDE.deserializer(), CAR_STATE_CHANGED_SERDE.deserializer());

        long now = Instant.EPOCH.toEpochMilli();

        String carID = "123";
        String plate = "ABCDEF";
        var event = CarCamEventBuilder.CarCamEvent(carID, "new", plate, "DEU", now, "front", "FFF", 0.6f, "out");


        // original plate
        inputTopic.pipeInput("hi", event, now);

        // update to original plate, same value
        inputTopic.pipeInput("hi", event.withCarState("update"), now + 1000);

        // update same value, new carID
        String carID2 = "234";
        inputTopic.pipeInput("hi", event.withCarID(carID2).withCarState("update"), now + 2000);

        // update totally different value
        String plateDifferent = "SDJFSJS";
        inputTopic.pipeInput("hi", event.withPlateUTF8(plateDifferent).withCarState("update"), now + 3000);

        // update similar value
        String plateSimlar = "ABCDEG";
        inputTopic.pipeInput("hi", event.withPlateUTF8(plateSimlar).withCarState("update"), now + 4000);


        var kvs = outputTopic.readKeyValuesToList();
        kvs.forEach(k -> log.info(k.toString()));

        testDriver.close();
    }


    @Test
    void initTest() {

        log.info("--------------");
        log.info("--------------");
        System.out.println("--------------");
        System.out.println("--------------");
        System.out.println("--------------");

        Topology topology = KennzeichenTopologyProducer.createTopology(inputTopicName, outputTopicName);

        System.out.println(topology.describe().toString());
        log.info(topology.describe().toString());

        var testDriver = new TopologyTestDriver(topology, props);

        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(inputTopicName, STRING_SERDE.serializer(), Serdes.String().serializer());
        //TestInputTopic<String, RawCarCamEventRoot> inputTopic = testDriver.createInputTopic(inputTopicName, CarCamEventTopologyProducer.stringSerde.serializer(), CarCamEventTopologyProducer.rawEventSerde.serializer());

        TestOutputTopic<String, CarStateChanged> outputTopic = testDriver.createOutputTopic(outputTopicName, STRING_SERDE.deserializer(), CAR_STATE_CHANGED_SERDE.deserializer());

        long now = Instant.EPOCH.toEpochMilli();

        PodamFactory eventFactory = new PodamFactoryImpl();
        var root = eventFactory.manufacturePojoWithFullData(RawCarCamEventRoot.class);

        Buffer buffer = root.buffer();

        RawCarCamEventRoot validRoot = root.withBuffer(buffer.withCarID("1234").withCapture_ts("1232345").withCapture_timestamp("230482034823").withPlateConfidence("0.6").withCarState("new"));

        byte[] rawEventBytes = RAW_CAR_CAM_EVENT_ROOT_SERDE.serializer().serialize(inputTopicName, validRoot);

        String validRootString = new String(rawEventBytes);


        List<RawCarCamEventRoot> rawEventList = List.of(validRoot);

        byte[] rawEventListBytes = RAW_EVENT_LIST_SERDE.serializer().serialize(inputTopicName, rawEventList);

        String rawEventListString = new String(rawEventListBytes);

        String rawEventListString2 = "[" + validRootString + "]";

        System.out.println("raw event list strings");
        System.out.println(rawEventListString);
        System.out.println(rawEventListString2);


        List<RawCarCamEventRoot> eventList = RAW_EVENT_LIST_SERDE.deserializer().deserialize(inputTopicName, rawEventListString.getBytes(StandardCharsets.UTF_8));

        log.info("received list of events:");
        eventList.forEach(log::info);

        //inputTopic.pipeInput("hi", rawEventListString, now);

        //var kvs = outputTopic.readKeyValuesToList();
        // kvs.forEach(k -> log.info(k.toString()));
        testDriver.close();
    }
}
