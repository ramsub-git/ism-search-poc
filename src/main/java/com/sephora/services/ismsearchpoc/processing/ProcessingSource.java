package com.sephora.services.ismsearchpoc.processing;

public enum ProcessingSource {
    KAFKA("KAFKA"),
    SCHEDULER("SCHEDULER"),
    FILE("FILE"),
    API("API"),
    OUTBOUND("OUTBOUND"),
    MANUAL("MANUAL");
    private final String value;

    ProcessingSource(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}