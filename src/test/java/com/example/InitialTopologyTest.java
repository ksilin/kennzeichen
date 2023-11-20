package com.example;

import io.quarkus.test.junit.QuarkusTest;

import org.apache.kafka.streams.*;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Properties;

@QuarkusTest
class InitialTopologyTest {

static String inputTopicName = "carCameraEvents";
static String outputTopicName = "carEventNotifications";

    static final Properties props = new Properties();

    Logger log = Logger.getLogger(InitialTopologyTest.class);

    @BeforeAll
    static void beforeAll(){
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "car-camera-stream-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
    }


    @Test
    void test() {

       Topology topology = CarCamEventTopologyBuilder.createTopoology(inputTopicName, outputTopicName);

        System.out.println(topology.describe().toString());

        var testDriver = new TopologyTestDriver(topology, props);

        TestInputTopic<String, CarCameraEvent> inputTopic = testDriver.createInputTopic(inputTopicName, CarCamEventTopologyBuilder.stringSerde.serializer(), CarCamEventTopologyBuilder.carCameraEventSerde.serializer());

        TestOutputTopic<String, CarStateChanged> outputTopic = testDriver.createOutputTopic(outputTopicName, CarCamEventTopologyBuilder.stringSerde.deserializer(), CarCamEventTopologyBuilder.carStateChangedSerde.deserializer());

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
