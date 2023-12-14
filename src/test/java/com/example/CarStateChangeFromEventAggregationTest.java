package com.example;

import com.example.model.*;
import io.quarkus.test.junit.QuarkusTest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
class CarStateChangeFromEventAggregationTest {

    Logger log = Logger.getLogger(CarStateChangeFromEventAggregationTest.class);

    public static final String IN = "EINFAHRT";
    public static final String OUT = "AUSFAHRT";


    @Test
    void happyCaseOuterSensorsFirstThenInnerSensors() {
        // {"ts":1701437161585,"state":"new","plate":"XXYYZZ","cam":"einfahrt_front","dir":"unknown"}
        // {"ts":1701437161705,"state":"update","plate":"XXYYZZ","cam":"einfahrt_front","dir":"in"}
        // {"ts":1701437173126,"state":"new","plate":"XXYYZZ","cam":"einfahrt_heck","dir":"unknown"}
        // {"ts":1701437173166,"state":"update","plate":"XXYYZZ","cam":"einfahrt_heck","dir":"in"}

        var now = Instant.now().toEpochMilli();

        var eventFrontUnknownNew = CarCamEventBuilder.CarCamEvent("1", "new", "ABCDEF", "DEU", now, "einfahrt_front", "GAZ", 0.7f, "unknown");
        var eventFrontInUpdate = eventFrontUnknownNew.withCarMoveDirection("in").withCarState("update").withCaptureTimestamp(now + 1000);
        var eventHeckUnknownNew = eventFrontUnknownNew.withSensorProviderID("einfahrt_heck").withCaptureTimestamp(now + 10000);
        var eventHeckInUpdate = eventFrontUnknownNew.withCarMoveDirection("in").withCarState("update").withSensorProviderID("einfahrt_heck").withCaptureTimestamp(now + 11000);

        CarCamEventAggregation agg = CarCamEventAggregationBuilder.CarCamEventAggregation(List.of(eventFrontUnknownNew, eventFrontInUpdate, eventHeckUnknownNew, eventHeckInUpdate));

        var carStateChange = carStateFromAggregation(agg);

        assertThat(carStateChange.newState()).isEqualTo(IN);
    }


    private CarStateChanged carStateFromAggregation(CarCamEventAggregation agg) {

        var events = agg.events();

        var directions = events.stream().map(CarCamEvent::carMoveDirection).filter(e -> !e.equals("unknown")).toList();

        String state = "UNKNOWN";
        if (!directions.isEmpty()) {
            state = directions.getFirst().equals("in") ? IN : OUT;
        }

        // TODO - count by group
        return CarStateChangedBuilder.CarStateChanged(agg.events().get(0).plateUTF8(), state);
    }


}
