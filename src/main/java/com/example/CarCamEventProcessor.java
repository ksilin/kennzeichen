package com.example;

import com.example.model.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jboss.logging.Logger;

import info.debatty.java.stringsimilarity.JaroWinkler;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.example.KennzeichenTopologyNames.PER_PLATE_STORE;

public class CarCamEventProcessor implements Processor<String, CarCamEvent, String, CarStateChanged> {

    Logger log = Logger.getLogger(CarCamEventProcessor.class);

    private ProcessorContext<String, CarStateChanged> ctx;
    private KeyValueStore<String, CarCamEventAggregation> perPlateStore;

    private final JaroWinkler jw = new JaroWinkler();

    @Override
    public void init(ProcessorContext<String, CarStateChanged> context) {
        Processor.super.init(context);
        ctx = context;
        perPlateStore = context.getStateStore(PER_PLATE_STORE);
    }

    @Override
    public void process(Record<String, CarCamEvent> record) {

        log.warn("processing event " + record.value().toString());

        String plateUtf8 = record.value().plateUTF8();

        CarCamEventAggregation perPlateAggregation = perPlateStore.get(plateUtf8);

        String carState = record.value().carState();

        if (carState.equals("new")) {
            if (perPlateAggregation == null) {
                log.infof("plate %s new", plateUtf8);
                perPlateAggregation = CarCamEventAggregationBuilder.CarCamEventAggregation(List.of(record.value()));
            } else {
                log.infof("plate %s new, but aggregation exists: ", plateUtf8, perPlateAggregation.events().stream().map(CarCamEvent::sensorProviderID).collect(Collectors.toUnmodifiableSet()));
                perPlateAggregation.events().add(record.value());
                perPlateAggregation = CarCamEventAggregationBuilder.from(perPlateAggregation).withEvents(perPlateAggregation.events());
            }
            perPlateStore.put(plateUtf8, perPlateAggregation);
            log.warn("per plate aggregation size " + perPlateAggregation.events().size());
        } else if (carState.equals("update")) {

            log.infof("update for %s, aggregation found: %b ", plateUtf8, perPlateAggregation != null);

            if (perPlateAggregation == null) {

                AtomicReference<CarCamEventAggregation> bestMatch = new AtomicReference<>();
                AtomicReference<Double> bestSimilarity = new AtomicReference<>(0.0);

                try (var it = perPlateStore.all()) {
                    it.forEachRemaining(entry -> {
                                            double sim = jw.similarity(plateUtf8, entry.key);
                                            if (sim > bestSimilarity.get()) {
                                                bestSimilarity.set(sim);
                                                bestMatch.set(entry.value);
                                            }
                                        }
                    );
                }
                //
                if (bestMatch.get() != null && !bestMatch.get().events().isEmpty()) {
                    log.infof("similarity between %s and %s %s", plateUtf8, bestMatch.get().events().get(0).plateUTF8(), bestSimilarity.get().toString());
                }
            } else {
                log.infof("plate %s updated, and aggregation exists: ", plateUtf8, perPlateAggregation.events().stream().map(CarCamEvent::sensorProviderID).collect(Collectors.toUnmodifiableSet()));
                log.infof("aggregation carIds: %s", perPlateAggregation.events().stream().map(CarCamEvent::carID).collect(Collectors.toUnmodifiableSet()).toString());
                perPlateAggregation.events().add(record.value());
                perPlateAggregation = CarCamEventAggregationBuilder.from(perPlateAggregation).withEvents(perPlateAggregation.events());
            }

        } else if (carState.equals("lost")) {
            log.warnv("lost car state for plate {0}", plateUtf8);
        } else {
            log.errorv("unknown car state {0} for plate ", carState, record.value().plateUTF8());
        }

        // TODO - CarStateChanged produced later downstream
        //        if (perPlateAggregation != null && perPlateAggregation.events().size() > 2) {
        //            Record<String, CarStateChanged> rec = new Record<>(plateUtf8, CarStateChangedBuilder.CarStateChanged(plateUtf8, "ENTERED"), ctx.currentStreamTimeMs());
        //            ctx.forward(rec);
        //        }
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
