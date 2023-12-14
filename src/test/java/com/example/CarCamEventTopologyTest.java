package com.example;

import com.example.model.*;
import io.quarkus.test.junit.QuarkusTest;

import org.apache.kafka.streams.*;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

import static com.example.KennzeichenSerdes.*;
import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
class CarCamEventTopologyTest {

    static String inputTopicName = "carCameraEvents";
    static String outputTopicName = "carEventNotifications";

    PodamFactory mockPojoFactory = new PodamFactoryImpl();

    static final Properties props = new Properties();

    Logger log = Logger.getLogger(CarCamEventTopologyTest.class);

    @BeforeAll
    static void beforeAll() {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "car-camera-stream-processor-v0.47");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
    }

    @Test
    void mustFindMostSimilarPlates() {

        List<String> plateChanges = List.of("ABCDEG", "BCDEF", "BCDE", "FEDCBA", "BBCDEG");

        Topology topo = KennzeichenTopologyProducer.createTopology(inputTopicName, outputTopicName);

        TopologyTestDriver testDriver = new TopologyTestDriver(topo, props);
        TestInputTopic<String, RawCarCamEventRoot> inputTopic = testDriver.createInputTopic(inputTopicName, STRING_SERDE.serializer(), RAW_CAR_CAM_EVENT_ROOT_SERDE.serializer());
        TestOutputTopic<String, CarStateChanged> outputTopic = testDriver.createOutputTopic(outputTopicName, STRING_SERDE.deserializer(), CAR_STATE_CHANGED_SERDE.deserializer());

        String carID = "123";
        String plate = "ABCDEF";
        long now = Instant.EPOCH.toEpochMilli();
        String nowString = Long.toString(now);
        RawCarCamEventRoot validEventRoot = makeValidEventRoot(nowString, carID, plate);
        Buffer mockBuffer = validEventRoot.buffer();

        inputTopic.pipeInput("hi", validEventRoot, now);

        IntStream.range(0, plateChanges.size())
                .forEach(index -> {
                             inputTopic.pipeInput("hi", validEventRoot.withBuffer(mockBuffer.withCarState("update").withPlateUTF8(plateChanges.get(index))), now + 1000 * index);
                             testDriver.advanceWallClockTime(Duration.ofSeconds(1));
                         }
                );

        log.info("resulting events:");
        var kvs = outputTopic.readKeyValuesToList();
        kvs.forEach(k -> log.info(k.toString()));

        testDriver.close();
    }

    @Test
    void plateCarIdDivergenceTest() {
        Topology topology = KennzeichenTopologyProducer.createTopology(inputTopicName, outputTopicName);

        var testDriver = new TopologyTestDriver(topology, props);

        TestInputTopic<String, RawCarCamEventRoot> inputTopic = testDriver.createInputTopic(inputTopicName, STRING_SERDE.serializer(), RAW_CAR_CAM_EVENT_ROOT_SERDE.serializer());

        TestOutputTopic<String, CarStateChanged> outputTopic = testDriver.createOutputTopic(outputTopicName, STRING_SERDE.deserializer(), CAR_STATE_CHANGED_SERDE.deserializer());

        long now = Instant.EPOCH.toEpochMilli();
        String nowString = Long.toString(now);

        String key = "hi";
        String carID = "123";
        String plate = "ABCDEF";

        RawCarCamEventRoot validEventRoot = makeValidEventRoot(nowString, carID, plate);
        Buffer mockBuffer = validEventRoot.buffer();

        // original plate
        inputTopic.pipeInput(key, validEventRoot, now);

        //update to original plate, same value
        inputTopic.pipeInput(key, validEventRoot.withBuffer(mockBuffer.withCarState("update")), now + 1000);

        // update same value, new carID
        String carID2 = "234";
        inputTopic.pipeInput(key, validEventRoot.withBuffer(mockBuffer.withCarID(carID2).withCarState("update")), now + 2000);

        // update totally different value
        String plateDifferent = "SDJFSJS";
        inputTopic.pipeInput(key, validEventRoot.withBuffer(mockBuffer.withPlateUTF8(plateDifferent).withCarState("update")), now + 3000);

        // update similar value
        String plateSimilar = "ABCDEG";
        inputTopic.pipeInput(key, validEventRoot.withBuffer(mockBuffer.withPlateUTF8(plateSimilar).withCarState("update")), now + 4000);


        var kvs = outputTopic.readKeyValuesToList();
        kvs.forEach(k -> log.info(k.toString()));

        testDriver.close();
    }


    @Test
    void rawCarCamEventRootListSerdeTest() {

        long now = Instant.EPOCH.toEpochMilli();
        String nowString = Long.toString(now);
        String carID = "123";
        String plate = "ABCDEF";

        RawCarCamEventRoot validEventRoot = makeValidEventRoot(nowString, carID, plate);

        List<RawCarCamEventRoot> rawEventList = List.of(validEventRoot);

        byte[] rawEventListBytes = RAW_EVENT_LIST_SERDE.serializer().serialize(inputTopicName, rawEventList);

        List<RawCarCamEventRoot> eventList = RAW_EVENT_LIST_SERDE.deserializer().deserialize(inputTopicName, rawEventListBytes);//rawEventListString.getBytes(StandardCharsets.UTF_8));

        assertThat(eventList).hasSize(1);
        assertThat(eventList.getFirst()).isEqualTo(validEventRoot);
    }

    RawCarCamEventRoot makeValidEventRoot(String timestampString, String carID, String plate){
        RawCarCamEventRoot rawEventRoot = mockPojoFactory.manufacturePojoWithFullData(RawCarCamEventRoot.class);
        Buffer validBuffer = rawEventRoot.buffer().withCapture_ts(timestampString).withPlateConfidence("0.8");
        Buffer mockBuffer = validBuffer.withCarID(carID).withPlateUTF8(plate);
        return rawEventRoot.withBuffer(mockBuffer);
    }
}
