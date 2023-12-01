package com.example;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Map;

public class CarCamEventTopologyProducer {

    public static final Serde<CarCamEvent> carCameraEventSerde = new ObjectMapperSerde<>(CarCamEvent.class);
    public static final Serde<CarStateChanged> carStateChangedSerde = new ObjectMapperSerde<>(CarStateChanged.class);
    public static final Serde<CarCamEventAggregation> carCamEventAggregationSerde = new ObjectMapperSerde<>(CarCamEventAggregation.class);

    public static final Serde<String> stringSerde = Serdes.String();

    public static final String PER_PLATE_STORE = "per_plate_store";
    static public Topology createTopoology(String inputTopicName, String outputTopicName){

        KeyValueBytesStoreSupplier perPlateStoreSupplier = Stores.persistentKeyValueStore(PER_PLATE_STORE);

        StoreBuilder<KeyValueStore<String, CarCamEventAggregation>> perPlateStoreBuilder = Stores.keyValueStoreBuilder(perPlateStoreSupplier, stringSerde, carCamEventAggregationSerde);

        var builder = new StreamsBuilder();
        builder.addStateStore(perPlateStoreBuilder);

        KStream<String, CarCamEvent> stream = builder.stream(inputTopicName, Consumed.with(stringSerde, carCameraEventSerde));

        KStream<String, CarCamEvent> stringCarCameraEventKStream = stream.selectKey((k, v) -> v.sensorNdl());
        KStream<String, CarCamEvent> repartitioned = stringCarCameraEventKStream.repartition(Repartitioned.with(stringSerde, carCameraEventSerde));// add Repartitioned if required
        var peeked = repartitioned.peek((k, v) -> System.out.println("key: " + k + " value: " + v));

        String splitName = "splitProcessor-";
        BranchedKStream<String, CarCamEvent> splitProcessor = peeked.split(Named.as(splitName));
        String lowConfidenceBranchName = "lowConfidenceBranch";
        BranchedKStream<String, CarCamEvent> lowConfidenceBranch = splitProcessor
                .branch((k, v) -> v.plateConfidence() < 0.6f, Branched.withConsumer( (lowConfidenceStream -> lowConfidenceStream.to("lowconfidenceTopic"))));
        String defaultBranchName = "defaultBranch";
        Map<String, KStream<String, CarCamEvent>> highConfidenceBranch = lowConfidenceBranch.defaultBranch(Branched.as(defaultBranchName));


        KStream<String, CarCamEvent> defaultBranch = highConfidenceBranch.get(splitName + defaultBranchName);

        KStream<String, CarStateChanged> processed = defaultBranch.process(CarCamEventProcessor::new, PER_PLATE_STORE);

        processed.to(outputTopicName, Produced.with(stringSerde, carStateChangedSerde));
        Topology topology = builder.build();

        return topology;
    }


}
