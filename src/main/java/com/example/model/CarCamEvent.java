package com.example.model;

import com.example.model.CarCamEventBuilder;
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

    public static CarCamEvent fromRawEvent(Root rawEvent){
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


}
