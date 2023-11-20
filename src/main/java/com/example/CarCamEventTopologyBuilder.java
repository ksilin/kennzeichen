package com.example;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class CarCamEventTopologyBuilder {

    public static final Serde<CarCameraEvent> carCameraEventSerde = new ObjectMapperSerde<>(CarCameraEvent.class);
    public static final Serde<CarStateChanged> carStateChangedSerde = new ObjectMapperSerde<>(CarStateChanged.class);
    public static final Serde<CarCamEventAggregation> carCamEventAggregationSerde = new ObjectMapperSerde<>(CarCamEventAggregation.class);

    public static final Serde<String> stringSerde = Serdes.String();

    public static final String PER_PLATE_STORE = "per_plate_store";
    public static final String PER_CARID_STORE = "per_carid_store";
    static public Topology createTopoology(String inputTopicName, String outputTopicName){

        KeyValueBytesStoreSupplier keyValueBytesStoreSupplier = Stores.persistentKeyValueStore(PER_PLATE_STORE);

        StoreBuilder<KeyValueStore<String, CarCamEventAggregation>> keyValueStoreStoreBuilder = Stores.keyValueStoreBuilder(keyValueBytesStoreSupplier, stringSerde, carCamEventAggregationSerde);

        var builder = new StreamsBuilder();
        builder.addStateStore(keyValueStoreStoreBuilder);

        KStream<String, CarCameraEvent> stream = builder.stream(inputTopicName, Consumed.with(stringSerde, carCameraEventSerde));

        KStream<String, CarCameraEvent> stringCarCameraEventKStream = stream.selectKey((k, v) -> v.sensorNdl());
        KStream<String, CarCameraEvent> repartitioned = stringCarCameraEventKStream.repartition(Repartitioned.with(stringSerde, carCameraEventSerde));// add Repartitioned if required
        var peeked = repartitioned.peek((k, v) -> System.out.println("key: " + k + " value: " + v));
        KStream<String, CarStateChanged> processed = peeked.process(CarCamEventProcessor::new, PER_PLATE_STORE);

        processed.to(outputTopicName, Produced.with(stringSerde, carStateChangedSerde));
        Topology topology = builder.build();

        return topology;
    }


}
