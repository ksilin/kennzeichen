package com.example.util;

import com.example.model.CarCamEventAggregation;

import java.util.Comparator;
import java.util.TreeMap;

public class SimilarPlates {

    int capacity;

    private final TreeMap<Double, CarCamEventAggregation> aggregationsForSimilarPlates;

    public SimilarPlates(int capacity) {
        this.capacity = capacity;
        aggregationsForSimilarPlates = new TreeMap<>(Comparator.reverseOrder());
    }

    public void add(Double similarity, CarCamEventAggregation aggregation) {
        aggregationsForSimilarPlates.put(similarity, aggregation);
        if (aggregationsForSimilarPlates.size() > capacity) {
            aggregationsForSimilarPlates.remove(aggregationsForSimilarPlates.lastKey());
        }
    }

    public TreeMap<Double, CarCamEventAggregation> getAggregationsForSimilarPlates() {
        return aggregationsForSimilarPlates;
    }
}
