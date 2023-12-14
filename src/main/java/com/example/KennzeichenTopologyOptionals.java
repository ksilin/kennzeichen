package com.example;

import com.example.model.CarCamEvent;
import com.example.model.CarStateChanged;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;

import static com.example.KennzeichenSerdes.*;
import static com.example.KennzeichenTopologyNames.REPARTITION_BY_NIEDERLASSUNG;

public class KennzeichenTopologyOptionals {

    public static final Repartitioned<String, CarCamEvent> carCamEventRepartitioned = Repartitioned.with(STRING_SERDE, CAR_CAM_EVENT_SERDE).withName(REPARTITION_BY_NIEDERLASSUNG);
    public static final Consumed<String, CarCamEvent> carCamEventConsumed = Consumed.with(STRING_SERDE, CAR_CAM_EVENT_SERDE);
    public static final Produced<String, CarCamEvent> carCamEventProduced = Produced.with(STRING_SERDE, CAR_CAM_EVENT_SERDE);
    public static final Produced<String, CarStateChanged> carStateChangedProduced = Produced.with(STRING_SERDE, CAR_STATE_CHANGED_SERDE);
}
