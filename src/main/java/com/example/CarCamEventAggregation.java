package com.example;

import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.List;

@RecordBuilder
public record CarCamEventAggregation(List<CarCameraEvent> events) implements CarCamEventAggregationBuilder.With {
    public static CarCamEventAggregation empty(){
        return new CarCamEventAggregation(List.of());
    }
}
