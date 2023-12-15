package com.example.util;

import com.example.model.CarCamEvent;
import com.example.model.CarCamEventAggregation;
import org.junit.jupiter.api.Test;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
public class SimilarPlatesTest {

    PodamFactory mockPojoFactory = new PodamFactoryImpl();
    Random random = new Random();

    @Test
    public void testAdd() {

        var capacity = 3;

        SimilarPlates plates = new SimilarPlates(capacity);

        var event = mockPojoFactory.manufacturePojoWithFullData(CarCamEvent.class);
        var events = List.of(event);
        CarCamEventAggregation aggregation = new CarCamEventAggregation(events);

        var topSimilarity = 0.0;
        for (int i = 0; i < 100; i++) {
            var similarity = random.nextDouble();
            if(similarity > topSimilarity) {
                topSimilarity = similarity;
            }
            plates.add(similarity, aggregation);
        }
        TreeMap<Double, CarCamEventAggregation> aggregations = plates.getAggregationsForSimilarPlates();
        assertEquals(capacity, aggregations.size());
        assertThat(aggregations.firstKey()).isEqualTo(topSimilarity);
        assertEquals(aggregation, aggregations.get(topSimilarity));
    }


}
