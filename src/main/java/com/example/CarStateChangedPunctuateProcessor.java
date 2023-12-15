package com.example;

import com.example.model.CarCamEvent;
import com.example.model.CarCamEventAggregation;
import com.example.model.CarStateChanged;
import com.example.model.CarStateChangedBuilder;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.example.KennzeichenTopologyNames.PER_PLATE_STORE_NAME;

public class CarStateChangedPunctuateProcessor implements Processor<String, CarCamEvent, String, CarStateChanged> {

    Logger log = Logger.getLogger(CarStateChangedPunctuateProcessor.class);

    private ProcessorContext<String, CarStateChanged> ctx;
    private KeyValueStore<String, CarCamEventAggregation> perPlateStore;
    public Long eventTimeoutThreshold;

    public CarStateChangedPunctuateProcessor(Long eventTimeoutThreshold) {
        this.eventTimeoutThreshold = eventTimeoutThreshold;
    }


    @Override
    public void init(ProcessorContext<String, CarStateChanged> context) {
        Processor.super.init(context);
        ctx = context;
        perPlateStore = context.getStateStore(PER_PLATE_STORE_NAME);

        Punctuator punctuator = timestamp -> {
            log.info("Running punctuator at " + timestamp);

            try (var it = perPlateStore.all()) {
                it.forEachRemaining(aggKV -> {
                    long lastEventTimestamp = aggKV.value.lastEventTimestamp();
                    var eventTimedOut = timestamp - lastEventTimestamp > eventTimeoutThreshold;

                    // TODO - add real logic for timeout case
                    if (eventTimedOut) {
                        log.warnv("event chain timed out. Status change event will be dispatched: {0}", aggKV);

                        // TODO - extract direction
                        List<String> directions = aggKV.value.events().stream().map(CarCamEvent::carMoveDirection).toList();

                        String state = directionToState(directions);

                        Record<String, CarStateChanged> rec = new Record<>(aggKV.key, CarStateChangedBuilder.CarStateChanged(aggKV.key, state), timestamp);
                        ctx.forward(rec);
                        perPlateStore.delete(aggKV.key);
                    }
                });
            }
        };
        ctx.schedule(Duration.ofSeconds(3), PunctuationType.WALL_CLOCK_TIME, punctuator);
    }

    @Override
    public void process(Record<String, CarCamEvent> record) {
        log.debugv("punctuator process. ignoring record: {0}", record);
    }

    // TODO - test
    String directionToState(List<String> directions) {

        Map<String, Long> countedDriections = directions.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        String state = countedDriections.entrySet().stream().max(Map.Entry.comparingByValue()).map(max ->  max.getKey().equals("in") ? "ENTERED" : "EXITED" ).orElse("UNKNOWN");
       return state;
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
