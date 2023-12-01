package com.example.model;

import com.example.model.CarCamEventAggregationBuilder;
import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@RecordBuilder
public record CarCamEventAggregation(List<CarCamEvent> events) implements CarCamEventAggregationBuilder.With {
    public static CarCamEventAggregation empty() {
        return new CarCamEventAggregation(List.of());
    }

    public static CarCamEventAggregation from(List<CarCamEvent> events) {
        return new CarCamEventAggregation(events);
    }

    // TODO - interesting - if this method is an instance method, Jackson mapper fails.
    public static Set<String> getAllPlates(CarCamEventAggregation agg) {
        Set<String> allPlates = agg.events.stream().map(CarCamEvent::plateUTF8).collect(Collectors.toUnmodifiableSet());
        return allPlates;
        //    return events.stream().map(CarCameraEvent::plateUTF8).toList();
    }

    // (l, r) -> Long.compare(l.captureTimestamp(), r.captureTimestamp())
    public long lastEventTimestamp() {
        Optional<CarCamEvent> l = events.stream().max(Comparator.comparingLong(CarCamEvent::captureTimestamp)
        );
        return l.map(CarCamEvent::captureTimestamp).orElse(0L);
    }

}
