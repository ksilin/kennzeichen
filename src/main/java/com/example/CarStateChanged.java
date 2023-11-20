package com.example;

import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record CarStateChanged(String plateUTF8, String newState) implements CarStateChangedBuilder.With {


}
