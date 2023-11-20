package com.example;

import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record CarCameraEvent(String carID,
                             String carState,
                             String plateUTF8,
                             String plateCountry,
                             long captureTimestamp,
                             String sensorProviderID,
                             String sensorNdl,
                             Float plateConfidence,
                             String carMoveDirection) {
}
