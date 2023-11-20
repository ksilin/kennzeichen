package com.example;

import io.quarkus.test.junit.QuarkusTest;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Properties;

@QuarkusTest
class InitialTopologyTest {

static String inputTopicName = "carCameraEvents";
static String outputTopicName = "carEventNotifications";

    static final Properties props = new Properties();

    @BeforeAll
    static void beforeAll(){
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "car-camera-stream-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
    }


    @Test
    void test() {

        Serde<String> serde = Serdes.String();


        var builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(inputTopicName, Consumed.with(serde, serde));

        // logic goes here

        stream.to(outputTopicName, Produced.with(serde, serde));
        Topology topology = builder.build();

        System.out.println(topology.describe().toString());



        var testDriver = new TopologyTestDriver(topology, props);

        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(inputTopicName, serde.serializer(), serde.serializer());

        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(outputTopicName, serde.deserializer(), serde.deserializer());

        inputTopic.pipeInput("hi", "world");

        var kvs = outputTopic.readKeyValuesToList();
        kvs.forEach( k -> System.out.println(k));

    }
}
