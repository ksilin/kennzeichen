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
        return new CarCamEvent(        rawEvent.buffer().carID(),
                rawEvent.buffer().carState(),
                rawEvent.buffer().plateUTF8(),
                rawEvent.buffer().plateCountry(),
                Long.parseLong(rawEvent.buffer().capture_ts()),
                rawEvent.buffer().sensorProviderID(),
                "",
                Float.parseFloat(rawEvent.buffer().plateConfidence()),
                rawEvent.buffer().carMoveDirection()
        );
    }

    public static final String STATE_NEW = "new";
    public static final String STATE_UPDATE = "update";
    public static final String STATE_UNKNOWN = "unknown";


}
