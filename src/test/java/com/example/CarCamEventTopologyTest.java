package com.example;

import io.quarkus.test.junit.QuarkusTest;

import org.apache.kafka.streams.*;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Properties;

@QuarkusTest
class CarCamEventTopologyTest {

static String inputTopicName = "carCameraEvents";
static String outputTopicName = "carEventNotifications";

    static final Properties props = new Properties();

    Logger log = Logger.getLogger(CarCamEventTopologyTest.class);

    @BeforeAll
    static void beforeAll(){
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "car-camera-stream-processor-v0.47");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
    }

    @Test
    void plateCarIdDivergenceTest(){
        Topology topology = CarCamEventTopologyProducer.createTopoology(inputTopicName, outputTopicName);

        var testDriver = new TopologyTestDriver(topology, props);

        TestInputTopic<String, CarCameraEvent> inputTopic = testDriver.createInputTopic(inputTopicName, CarCamEventTopologyProducer.stringSerde.serializer(), CarCamEventTopologyProducer.carCameraEventSerde.serializer());

        TestOutputTopic<String, CarStateChanged> outputTopic = testDriver.createOutputTopic(outputTopicName, CarCamEventTopologyProducer.stringSerde.deserializer(), CarCamEventTopologyProducer.carStateChangedSerde.deserializer());

        long now = Instant.EPOCH.toEpochMilli();

        String carID = "123";
        String plate = "SDFPKSDSE";
        var event = CarCameraEventBuilder.CarCameraEvent(carID, "new", plate, "DEU", now, "front", "FFF", 0.6f, "out");


        inputTopic.pipeInput("hi",event, now);

        inputTopic.pipeInput("hi",event.withCarState("update"), now + 1000);


        String carID2 = "234";

        inputTopic.pipeInput("hi",event.withCarID(carID2).withCarState("update"), now + 2000);
        String plate2 = "SDJFSJS";
        inputTopic.pipeInput("hi",event.withPlateUTF8(plate2).withCarState("update"), now + 3000);


        var kvs = outputTopic.readKeyValuesToList();
        kvs.forEach( k -> log.info(k.toString()));
    }


    @Test
    void initTest() {

       Topology topology = CarCamEventTopologyProducer.createTopoology(inputTopicName, outputTopicName);

        System.out.println(topology.describe().toString());

        var testDriver = new TopologyTestDriver(topology, props);

        TestInputTopic<String, CarCameraEvent> inputTopic = testDriver.createInputTopic(inputTopicName, CarCamEventTopologyProducer.stringSerde.serializer(), CarCamEventTopologyProducer.carCameraEventSerde.serializer());

        TestOutputTopic<String, CarStateChanged> outputTopic = testDriver.createOutputTopic(outputTopicName, CarCamEventTopologyProducer.stringSerde.deserializer(), CarCamEventTopologyProducer.carStateChangedSerde.deserializer());

        long now = Instant.EPOCH.toEpochMilli();

        var event = CarCameraEventBuilder.CarCameraEvent("123", "update", "SDFPKSDSE", "DEU", now, "front", "FFF", 0.6f, "out");

        inputTopic.pipeInput("hi",event, now);
        inputTopic.pipeInput("hi",event, now + 1000);
        inputTopic.pipeInput("hi",event, now + 2000);
        inputTopic.pipeInput("hi",event, now + 3000);

        var kvs = outputTopic.readKeyValuesToList();
        kvs.forEach( k -> log.info(k.toString()));

    }
}
