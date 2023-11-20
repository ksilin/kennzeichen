package com.example;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jboss.logging.Logger;

import java.util.List;

public class CarCamEventProcessor implements Processor<String, CarCameraEvent, String, CarStateChanged> {

Logger log = Logger.getLogger(CarCamEventProcessor.class);

    private ProcessorContext ctx;
    private KeyValueStore<String, CarCamEventAggregation> perPlateStore;

    @Override
    public void init(ProcessorContext<String, CarStateChanged> context) {
        Processor.super.init(context);
        ctx = context;
        perPlateStore = context.getStateStore(CarCamEventTopologyBuilder.PER_PLATE_STORE);
    }

    @Override
    public void process(Record<String, CarCameraEvent> record) {

        log.warn("processing event " + record.value().toString());

        String plateUtf8 = record.value().plateUTF8();

        CarCamEventAggregation carCamEventAggregation = perPlateStore.get(plateUtf8);

        if(carCamEventAggregation == null){
            carCamEventAggregation = CarCamEventAggregationBuilder.CarCamEventAggregation(List.of(record.value()));
            perPlateStore.put(plateUtf8,carCamEventAggregation);
        } else {
            carCamEventAggregation.events().add(record.value());
            var newAgg = CarCamEventAggregationBuilder.from(carCamEventAggregation).withEvents(carCamEventAggregation.events());
            perPlateStore.put(plateUtf8,newAgg);
            log.warn("aggregation size " + newAgg.events().size());
        }

       if(carCamEventAggregation.events().size() > 2){
           Record<String, CarStateChanged> rec = new Record<>(plateUtf8, CarStateChangedBuilder.CarStateChanged(plateUtf8, "ENTERED"), ctx.currentStreamTimeMs());
           ctx.forward(rec);
       }

    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
