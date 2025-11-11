package com.sephora.services.ismsearchpoc.processing;

public enum SeverityLevel {
    P0("Critical - System Down"),
    P1("High - Data Loss Risk"),
    P2("Medium - Performance Impact"),
    P3("Low - Minor Issue");

    private final String description;

    SeverityLevel(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}