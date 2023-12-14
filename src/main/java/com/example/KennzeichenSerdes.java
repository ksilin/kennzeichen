package com.example;

import com.example.model.CarCamEvent;
import com.example.model.CarCamEventAggregation;
import com.example.model.CarStateChanged;
import com.example.model.RawCarCamEventRoot;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.List;

public class KennzeichenSerdes {

    public static final Serde<CarCamEvent> CAR_CAM_EVENT_SERDE = new ObjectMapperSerde<>(CarCamEvent.class);
    public static final Serde<CarStateChanged> CAR_STATE_CHANGED_SERDE = new ObjectMapperSerde<>(CarStateChanged.class);
    public static final Serde<CarCamEventAggregation> CAR_CAM_EVENT_AGGREGATION_SERDE = new ObjectMapperSerde<>(CarCamEventAggregation.class);
    public static final Serde<RawCarCamEventRoot> RAW_CAR_CAM_EVENT_ROOT_SERDE = new ObjectMapperSerde<>(RawCarCamEventRoot.class);

    private static final List<RawCarCamEventRoot> trickList = List.of();
    public static final Serde<List<RawCarCamEventRoot>> RAW_EVENT_LIST_SERDE = new ObjectMapperSerde(trickList.getClass());

    public static final Serde<String> STRING_SERDE = Serdes.String();
}
