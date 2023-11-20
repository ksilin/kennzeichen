package com.example;

import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.List;

@RecordBuilder
public record CarCamEventAggregation(List<CarCameraEvent> events) implements CarCamEventAggregationBuilder.With {
}
