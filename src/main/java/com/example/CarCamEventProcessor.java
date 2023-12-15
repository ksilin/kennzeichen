package com.example;

import com.example.model.*;
import com.example.util.SimilarPlates;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jboss.logging.Logger;

import info.debatty.java.stringsimilarity.JaroWinkler;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.example.KennzeichenTopologyNames.PER_PLATE_STORE_NAME;
import static com.example.model.CarCamEvent.STATE_NEW;
import static com.example.model.CarCamEvent.STATE_UPDATE;

public class CarCamEventProcessor implements Processor<String, CarCamEvent, String, CarStateChanged> {

    Logger log = Logger.getLogger(CarCamEventProcessor.class);

    private ProcessorContext<String, CarStateChanged> ctx;
    private KeyValueStore<String, CarCamEventAggregation> perPlateStore;

    private static final JaroWinkler jw = new JaroWinkler();

    @Override
    public void init(ProcessorContext<String, CarStateChanged> context) {
        Processor.super.init(context);
        ctx = context;
        perPlateStore = context.getStateStore(PER_PLATE_STORE_NAME);
    }

    @Override
    public void process(Record<String, CarCamEvent> record) {

        log.warn("processing event " + record.value().toString());

        String plateUtf8 = record.value().plateUTF8();

        CarCamEventAggregation perPlateAggregation = perPlateStore.get(plateUtf8);

        String carState = record.value().carState();

        if (carState.equals(STATE_NEW)) {
            if (perPlateAggregation == null) {
                log.infof("plate %s is new, creating a new aggregation", plateUtf8);
                perPlateAggregation = CarCamEventAggregationBuilder.CarCamEventAggregation(List.of(record.value()));
            } else {
                log.infof("event for plate %s has state new, but aggregation exists: %s", plateUtf8, perPlateAggregation.events().stream().map(CarCamEvent::sensorProviderID).collect(Collectors.toUnmodifiableSet()));
                perPlateAggregation = perPlateAggregation.withAddedEvent(record.value());
            }
            perPlateStore.put(plateUtf8, perPlateAggregation);
            log.warn("per plate aggregation size " + perPlateAggregation.events().size());
        } else if (carState.equals(STATE_UPDATE)) {

            log.infof("update for %s, aggregation found: %b ", plateUtf8, perPlateAggregation != null);

            if (perPlateAggregation == null) {

                var similarPlates = getSimilarPlates(plateUtf8, perPlateStore);
                TreeMap<Double, CarCamEventAggregation> aggregationsForSimilarPlates = similarPlates.getAggregationsForSimilarPlates();
                Map.Entry<Double, CarCamEventAggregation> firstEntry = aggregationsForSimilarPlates.firstEntry();

                if (firstEntry != null && !firstEntry.getValue().events().isEmpty()) {
                    String bestMatchPlate = firstEntry.getValue().events().get(0).plateUTF8();
                    log.infof("similarity between %s and %s %s", plateUtf8, bestMatchPlate, firstEntry.getKey());

                    // TODO - use cutoff if no plate is reasonably similar

                    log.infof("updating aggregation for %s with new event with plate %s ", bestMatchPlate, plateUtf8);
                    // TODO - evaluate if store is necessary or whether we just update in store
                    perPlateStore.put(bestMatchPlate,  firstEntry.getValue().withAddedEvent(record.value()));
                }
            } else {
                log.infof("plate %s updated, and aggregation exists: ", plateUtf8, perPlateAggregation.events().stream().map(CarCamEvent::sensorProviderID).collect(Collectors.toUnmodifiableSet()));
                log.infof("aggregation carIds: %s", perPlateAggregation.events().stream().map(CarCamEvent::carID).collect(Collectors.toUnmodifiableSet()).toString());
                perPlateAggregation.events().add(record.value());
                // TODO - check if assignment is necessary
                perPlateAggregation = CarCamEventAggregationBuilder.from(perPlateAggregation).withEvents(perPlateAggregation.events());
            }

        } else if (carState.equals("lost")) {
            log.warnv("lost car state for plate {0}, disregarding", plateUtf8);
        } else {
            log.errorv("unknown car state {0} for plate ", carState, record.value().plateUTF8());
        }
    }

    public static SimilarPlates getSimilarPlates(String plate, KeyValueStore<String, CarCamEventAggregation> store) {

        var similarPlates = new SimilarPlates(3);

        try (var it = store.all()) {
            it.forEachRemaining(entry -> similarPlates.add(jw.similarity(plate, entry.key), entry.value)
            );
        }
        return similarPlates;
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
