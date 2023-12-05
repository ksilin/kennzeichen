package com.example.model;

import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record Root(
        Integer size,
        Buffer buffer,
         String encoding,
         String mimetype,
         String fieldname,
        String originalname) implements RootBuilder.With {
}