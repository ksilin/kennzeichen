package com.example;

import com.example.model.CarCamEvent;
import com.example.model.CarCamEventAggregation;
import com.example.model.CarStateChanged;
import com.example.model.Root;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.Map;

@ApplicationScoped
public class CarCamEventTopologyProducer {

    static Logger log = Logger.getLogger(CarCamEventTopologyProducer.class);

    public static final Serde<CarCamEvent> carCameraEventSerde = new ObjectMapperSerde<>(CarCamEvent.class);
    public static final Serde<CarStateChanged> carStateChangedSerde = new ObjectMapperSerde<>(CarStateChanged.class);
    public static final Serde<CarCamEventAggregation> carCamEventAggregationSerde = new ObjectMapperSerde<>(CarCamEventAggregation.class);
    public static final Serde<Root> rawEventSerde = new ObjectMapperSerde<>(Root.class);

    public static Serde<CarStateChanged> CarStateChangedSerde = new ObjectMapperSerde<>(CarStateChanged.class);

    public static final Serde<String> stringSerde = Serdes.String();

    public static final String PER_PLATE_STORE = "per_plate_store";


    @Produces
    public Topology makeTopology(@ConfigProperty(name="kennzeichen.source-topic", defaultValue = "carCamEventsSource") String sourceTopic, @ConfigProperty(name="kennzeichen.target-topic", defaultValue = "carStatusChanged") String targetTopic){
        return createTopoology(sourceTopic, targetTopic);
    }

    static public Topology createTopoology(String inputTopicName, String outputTopicName){

        StoreBuilder<KeyValueStore<String, CarCamEventAggregation>> perPlateStoreBuilder = makePerPlateStore();

        var builder = new StreamsBuilder();
        builder.addStateStore(perPlateStoreBuilder);

        KStream<String, Root> streamRaw = builder.stream(inputTopicName, Consumed.with(stringSerde, rawEventSerde));

        KStream<String, CarCamEvent> stream = streamRaw.mapValues(e -> {
            log.infov("converting event from raw to minimal: {0}", e);
            return CarCamEvent.fromRawEvent(e);
        });

        KStream<String, CarCamEvent> stringCarCameraEventKStream = stream.selectKey((k, v) -> v.sensorNdl());
        KStream<String, CarCamEvent> repartitioned = stringCarCameraEventKStream.repartition(Repartitioned.with(stringSerde, carCameraEventSerde));// add Repartitioned if required
        var peeked = repartitioned.peek((k, v) -> System.out.println("key: " + k + " value: " + v));

        String splitName = "splitProcessor-";
        BranchedKStream<String, CarCamEvent> splitProcessor = peeked.split(Named.as(splitName));
        String lowConfidenceBranchName = "lowConfidenceBranch";
        BranchedKStream<String, CarCamEvent> lowConfidenceBranch = splitProcessor
                .branch((k, v) -> v.plateConfidence() < 0.6f, Branched.withConsumer( (lowConfidenceStream -> lowConfidenceStream.to("lowconfidenceTopic"))));
        String defaultBranchName = "defaultBranch";
        Map<String, KStream<String, CarCamEvent>> branchMap = lowConfidenceBranch.defaultBranch(Branched.as(defaultBranchName));


        KStream<String, CarCamEvent> highConfidenceBranch = branchMap.get(splitName + defaultBranchName);

        KStream<String, CarStateChanged> processed = highConfidenceBranch.process(CarCamEventProcessor::new, PER_PLATE_STORE);

        highConfidenceBranch.process(() -> new CarStatusChangedPunctuateProcessor(1000L), PER_PLATE_STORE);

        processed.to(outputTopicName, Produced.with(stringSerde, carStateChangedSerde));
        Topology topology = builder.build();

        return topology;
    }

    public static StoreBuilder<KeyValueStore<String, CarCamEventAggregation>> makePerPlateStore() {
        KeyValueBytesStoreSupplier perPlateStoreSupplier = Stores.persistentKeyValueStore(PER_PLATE_STORE);
        StoreBuilder<KeyValueStore<String, CarCamEventAggregation>> perPlateStoreBuilder = Stores.keyValueStoreBuilder(perPlateStoreSupplier, stringSerde, carCamEventAggregationSerde);
        return perPlateStoreBuilder;
    }


}
