package com.ticomgeo.fame.nifi;

import org.apache.nifi.processor.Relationship;

public class ResultEvents {
    public static final Relationship TIMER_EVENT = new Relationship.Builder()
            .name("Timer Event")
            .description("Timer Event")
            .build();
    public static final Relationship PUSHPOINT_REQUEST_EVENT = new Relationship.Builder()
            .name("Pushpoint Request")
            .description("PushPoint Request Event")
            .build();

    public static final Relationship UI_GEOLOCATION_QUERY = new Relationship.Builder()
            .name("UIGeolocation Query")
            .description("UIGeolocation Query")
            .build();
    public static final Relationship UI_GEOLOCATION_RESPONSE = new Relationship.Builder()
            .name("UIGeolocation Response")
            .description("UIGeolocation Response")
            .build();

    public static final Relationship GEOLOCATION_QUERY = new Relationship.Builder()
            .name("GeoLocation Query")
            .description("GeoLocation Query")
            .build();
    public static final Relationship GEOLOCATION_RESPONSE = new Relationship.Builder()
            .name("GeoLocation Response")
            .description("GeoLocation Response")
            .build();

    public static final Relationship APP_GEO_QUERY = new Relationship.Builder()
            .name("AppGeo Query")
            .description("AppGeo Query")
            .build();
    public static final Relationship APP_GEO_RESPONSE = new Relationship.Builder()
            .name("AppGeo Response")
            .description("AppGeo Response")
            .build();

    public static final Relationship UI_EVENT_GEO = new Relationship.Builder()
            .name("UI Event Geo")
            .description("UI Event Geo")
            .build();


    private ResultEvents() {}
}
