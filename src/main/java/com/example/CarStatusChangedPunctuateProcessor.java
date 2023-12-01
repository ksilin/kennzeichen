package com.example;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jboss.logging.Logger;

import java.time.Duration;

public class CarStatusChangedPunctuateProcessor implements Processor<String, CarCamEvent, String, CarStateChanged> {

    Logger log = Logger.getLogger(CarStatusChangedPunctuateProcessor.class);

    private ProcessorContext<String, CarStateChanged> ctx;
    private KeyValueStore<String, CarCamEventAggregation> perPlateStore;
    public Long eventTimeoutThreshold;

    public CarStatusChangedPunctuateProcessor(Long eventTimeoutThreshold) {
        this.eventTimeoutThreshold = eventTimeoutThreshold;
    }


    @Override
    public void init(ProcessorContext<String, CarStateChanged> context) {
        Processor.super.init(context);
        ctx = context;
        perPlateStore = context.getStateStore(CarCamEventTopologyProducer.PER_PLATE_STORE);

        Punctuator punctuator = timestamp -> {
            log.info("Running punctuator at " + timestamp);

            try(var it = perPlateStore.all()){
             it.forEachRemaining(aggKV -> {
                 var eventTimedOut = timestamp - aggKV.value.lastEventTimestamp() > eventTimeoutThreshold;

                 if(eventTimedOut){
                     log.warnv("event chain timed out. Status change event will be dispatched: {0}", aggKV);
                 }
                 //ctx.forward();
                 // perPlateStore.delete(aggKV.key);
             });
            }
        };
        ctx.schedule(Duration.ofSeconds(3), PunctuationType.WALL_CLOCK_TIME, punctuator);
    }

    @Override
    public void process(Record<String, CarCamEvent> record) {
        log.infov("punctuator process. ignoring record: {0}", record);
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}