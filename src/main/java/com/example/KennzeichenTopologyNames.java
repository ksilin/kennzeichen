package com.example;

public class KennzeichenTopologyNames {

    public static final String PER_PLATE_STORE_NAME = "per_plate_store";
    public static final String REPARTITION_BY_NIEDERLASSUNG = "repartitionByNiederlassung";
    public static final String CONFIDENCE_SPLIT_PROCESSOR_NAME = "confidenceSplitProcessor-";
    public static final String CAR_CAM_EVENT_PROCESSOR_NAME = "carCamEventProcessor";
    public static final String CAR_STATE_CHANGED_PUNCTUATE_PROCESSOR_NAME = "carStateChangedPunctuateProcessor";
    public static final String LOW_CONFIDENCE_BRANCH_NAME = "lowConfidenceCarCamEventsBranch";
    public static final String DEFAULT_BRANCH_NAME = "highConfidenceCarCamEventsBranch";
    public static final String LOW_CONFIDENCE_TOPIC_NAME = "lowConfidenceCarCamEvents";
}
