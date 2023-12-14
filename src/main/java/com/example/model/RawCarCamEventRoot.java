package com.example.model;

import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record RawCarCamEventRoot(
        Integer size,
        Buffer buffer,
        String encoding,
        String mimetype,
        String fieldname,
        String originalname) implements RawCarCamEventRootBuilder.With {
}