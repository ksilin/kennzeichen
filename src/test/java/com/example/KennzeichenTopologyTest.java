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
import static com.example.model.CarCamEvent.STATE_NEW;
import static com.example.model.CarCamEvent.STATE_UPDATE;
import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
class KennzeichenTopologyTest {

    static String inputTopicName = "carCameraEvents";
    static String outputTopicName = "carEventNotifications";

    PodamFactory mockPojoFactory = new PodamFactoryImpl();

    static final Properties props = new Properties();

    Logger log = Logger.getLogger(KennzeichenTopologyTest.class);

    @BeforeAll
    static void beforeAll() {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "car-camera-stream-processor-v0.47");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
    }

    @Test
    void mustFindMostSimilarPlates() {

        // similarity between ABCDEG and ABCDEF 0.9444444179534912
        // similarity between BCDEF and ABCDEF 0.9444444179534912
        // similarity between BCDE and ABCDEF 0.8888888955116272
        // similarity between FEDCBA and ABCDEF 0.3888889253139496
        // similarity between BBCDEG and ABCDEF 0.7777778506278992
        List<String> plateChanges = List.of("ABCDEG", "BCDEF", "BCDE", "FEDCBA", "BBCDEG");

        String carID = "123";
        String plate = "ABCDEF";
        long now = Instant.EPOCH.toEpochMilli();
        String nowString = Long.toString(now);
        Buffer validBuffer = makeValidEventBuffer(nowString, carID, plate).withCarState(STATE_NEW);

        try(var testDriver = new TopologyTestDriver(KennzeichenTopologyProducer.createTopology(inputTopicName, outputTopicName), props)){
            TestInputTopic<String, Buffer> inputTopic = testDriver.createInputTopic(inputTopicName, STRING_SERDE.serializer(), BUFFER_EVENT_SERDE.serializer());
            TestOutputTopic<String, CarStateChanged> outputTopic = testDriver.createOutputTopic(outputTopicName, STRING_SERDE.deserializer(), CAR_STATE_CHANGED_SERDE.deserializer());

            inputTopic.pipeInput("hi", validBuffer, now);
            IntStream.range(0, plateChanges.size())

                    .forEach(index -> {
                                 inputTopic.pipeInput("hi", validBuffer.withCarState(STATE_UPDATE).withPlateUTF8(plateChanges.get(index)), now + 10 * index);
                             }
                    );

            log.info("resulting events:");
            var kvs = outputTopic.readKeyValuesToList();
            kvs.forEach(k -> log.info(k.toString()));
        }
    }

    @Test
    void plateCarIdDivergenceTest() {

        long now = Instant.EPOCH.toEpochMilli();
        String nowString = Long.toString(now);

        String key = "hi";
        String carID = "123";
        String plate = "ABCDEF";

        Buffer validBuffer = makeValidEventBuffer(nowString, carID, plate);

        try(var testDriver = new TopologyTestDriver(KennzeichenTopologyProducer.createTopology(inputTopicName, outputTopicName), props)){

        TestInputTopic<String, Buffer> inputTopic = testDriver.createInputTopic(inputTopicName, STRING_SERDE.serializer(), BUFFER_EVENT_SERDE.serializer());
        TestOutputTopic<String, CarStateChanged> outputTopic = testDriver.createOutputTopic(outputTopicName, STRING_SERDE.deserializer(), CAR_STATE_CHANGED_SERDE.deserializer());

        // original plate
        inputTopic.pipeInput(key, validBuffer, now);
        //update to original plate, same value
        inputTopic.pipeInput(key, validBuffer.withCarState("update"), now + 1000);
        // update same value, new carID
        String carID2 = "234";
        inputTopic.pipeInput(key, validBuffer.withCarID(carID2).withCarState("update"), now + 2000);
        // update totally different value
        String plateDifferent = "SDJFSJS";
        inputTopic.pipeInput(key, validBuffer.withPlateUTF8(plateDifferent).withCarState("update"), now + 3000);

        // update similar value
        String plateSimilar = "ABCDEG";
        inputTopic.pipeInput(key, validBuffer.withPlateUTF8(plateSimilar).withCarState("update"), now + 4000);

        var kvs = outputTopic.readKeyValuesToList();
        kvs.forEach(k -> log.info(k.toString()));
        }
    }

    Buffer makeValidEventBuffer(String timestampString, String carID, String plate){
        Buffer rawEventRoot = mockPojoFactory.manufacturePojoWithFullData(Buffer.class);
        Buffer validBuffer = rawEventRoot.withCapture_ts(timestampString).withPlateConfidence("0.8");
        return validBuffer.withCarID(carID).withPlateUTF8(plate);
    }
}
