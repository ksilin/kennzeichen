package com.example.model;

import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record CarCamEvent(String carID,
                          String carState,
                          String plateUTF8,
                          String plateCountry,
                          long captureTimestamp,
                          String sensorProviderID,
                          String sensorNdl,
                          Float plateConfidence,
                          String carMoveDirection) implements CarCamEventBuilder.With {

    public static CarCamEvent fromRawEvent(RawCarCamEventRoot rawEvent){
        return fromBuffer(rawEvent.buffer());
    }

    // TODO - extract Niederlassung from sensorProviderID
    public static CarCamEvent fromBuffer(Buffer buffer) {
        return new CarCamEvent(buffer.carID(),
                               buffer.carState(),
                               buffer.plateUTF8(),
                               buffer.plateCountry(),
                               Long.parseLong(buffer.capture_ts()),
                               buffer.sensorProviderID(),
                               "",
                               Float.parseFloat(buffer.plateConfidence()),
                               buffer.carMoveDirection()
        );
    }

    public static final String STATE_NEW = "new";
    public static final String STATE_UPDATE = "update";
    public static final String STATE_UNKNOWN = "unknown";


}
