package com.example;

import com.example.model.CarCamEvent;
import com.example.model.CarCamEventAggregation;
import com.example.model.CarStateChanged;
import com.example.model.RawCarCamEventRoot;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.Map;

import static com.example.KennzeichenSerdes.*;
import static com.example.KennzeichenTopologyNames.*;
import static com.example.KennzeichenTopologyOptionals.*;

@ApplicationScoped
public class KennzeichenTopologyProducer {

    static Logger log = Logger.getLogger(KennzeichenTopologyProducer.class);

    public static final float plateConfidenceCutoff = 0.6f;
    public static long carCamEventTimeoutThresholdMs = 1000L;

    public static final Predicate<String, CarCamEvent> plateConfidencePredicate = (k, v) -> v.plateConfidence() < plateConfidenceCutoff;

    public static Branched<String, CarCamEvent> branchedToLowConfidenceTopic = Branched.withConsumer((lowConfidenceStream -> lowConfidenceStream.to(LOW_CONFIDENCE_TOPIC_NAME)), LOW_CONFIDENCE_BRANCH_NAME);


    @Produces
    public Topology makeTopology(@ConfigProperty(name = "kennzeichen.source-topic", defaultValue = "carCamEventsSource") String sourceTopic,
                                 @ConfigProperty(name = "kennzeichen.target-topic", defaultValue = "carStatusChanged") String targetTopic) {
        return createTopology(sourceTopic, targetTopic);
    }

    static public Topology createTopology(String inputTopicName, String outputTopicName) {

        StoreBuilder<KeyValueStore<String, CarCamEventAggregation>> perPlateStoreBuilder = makePerPlateStore();

        var builder = new StreamsBuilder();
        builder.addStateStore(perPlateStoreBuilder);

        KStream<String, RawCarCamEventRoot> streamRaw = builder.stream(inputTopicName, Consumed.with(STRING_SERDE, RAW_CAR_CAM_EVENT_ROOT_SERDE));

        KStream<String, CarCamEvent> stream = streamRaw.mapValues(CarCamEvent::fromRawEvent);

        KStream<String, CarCamEvent> stringCarCameraEventKStream = stream.selectKey((k, v) -> v.sensorNdl());
        KStream<String, CarCamEvent> repartitioned = stringCarCameraEventKStream.repartition(carCamEventRepartitioned);// add Repartitioned if required
        var peeked = repartitioned.peek((k, v) -> log.infov("key: {0}, value: {1}", k, v));

        BranchedKStream<String, CarCamEvent> splitProcessor = peeked.split(Named.as(CONFIDENCE_SPLIT_PROCESSOR));
        BranchedKStream<String, CarCamEvent> lowConfidenceBranch = splitProcessor
                .branch(plateConfidencePredicate, branchedToLowConfidenceTopic);
        Map<String, KStream<String, CarCamEvent>> branchMap = lowConfidenceBranch.defaultBranch(Branched.as(DEFAULT_BRANCH_NAME));


        KStream<String, CarCamEvent> highConfidenceBranch = branchMap.get(CONFIDENCE_SPLIT_PROCESSOR + DEFAULT_BRANCH_NAME);

        KStream<String, CarStateChanged> processed = highConfidenceBranch.process(CarCamEventProcessor::new, PER_PLATE_STORE);

        highConfidenceBranch.process(() -> new CarStatusChangedPunctuateProcessor(carCamEventTimeoutThresholdMs), PER_PLATE_STORE);

        processed.to(outputTopicName, carStateChangedProduced);

        return builder.build();
    }

    static public Topology createTopologyFluent(String inputTopicName, String outputTopicName) {

        var builder = new StreamsBuilder();
        builder.addStateStore(makePerPlateStore());

        KStream<String, CarCamEvent> highConfidenceBranch = builder.stream(inputTopicName, Consumed.with(STRING_SERDE, RAW_CAR_CAM_EVENT_ROOT_SERDE))
                .mapValues(CarCamEvent::fromRawEvent)
                .selectKey((k, v) -> v.sensorNdl())
                .repartition(carCamEventRepartitioned)
                .peek((k, v) -> log.infov("key: {0}, value: {1}", k, v)).split(Named.as(CONFIDENCE_SPLIT_PROCESSOR))
                .branch(plateConfidencePredicate, branchedToLowConfidenceTopic)
                .defaultBranch(Branched.as(DEFAULT_BRANCH_NAME))
                .get(CONFIDENCE_SPLIT_PROCESSOR + DEFAULT_BRANCH_NAME);

        KStream<String, CarStateChanged> processed = highConfidenceBranch.process(CarCamEventProcessor::new, PER_PLATE_STORE);

        ProcessorSupplier<String, CarCamEvent, String, CarStateChanged> carStatusChangedPunctuateProcessorSupplier =
                () -> new CarStatusChangedPunctuateProcessor(carCamEventTimeoutThresholdMs);
        highConfidenceBranch.process(carStatusChangedPunctuateProcessorSupplier, PER_PLATE_STORE);

        processed.to(outputTopicName, carStateChangedProduced);

        return builder.build();
    }


    public static StoreBuilder<KeyValueStore<String, CarCamEventAggregation>> makePerPlateStore() {
        KeyValueBytesStoreSupplier perPlateStoreSupplier = Stores.persistentKeyValueStore(PER_PLATE_STORE);
        StoreBuilder<KeyValueStore<String, CarCamEventAggregation>> perPlateStoreBuilder =
                Stores.keyValueStoreBuilder(perPlateStoreSupplier, STRING_SERDE, CAR_CAM_EVENT_AGGREGATION_SERDE);
        return perPlateStoreBuilder;
    }


}
