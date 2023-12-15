package com.example;

import com.example.model.*;
import com.example.util.ListOfRawCarCamEventRootsSerde;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.List;

public class KennzeichenSerdes {

    public static final Serde<CarCamEvent> CAR_CAM_EVENT_SERDE = new ObjectMapperSerde<>(CarCamEvent.class);

    public static final Serde<Buffer> BUFFER_EVENT_SERDE = new ObjectMapperSerde<>(Buffer.class);
    public static final Serde<CarStateChanged> CAR_STATE_CHANGED_SERDE = new ObjectMapperSerde<>(CarStateChanged.class);
    public static final Serde<CarCamEventAggregation> CAR_CAM_EVENT_AGGREGATION_SERDE = new ObjectMapperSerde<>(CarCamEventAggregation.class);
    public static final Serde<String> STRING_SERDE = Serdes.String();
}
