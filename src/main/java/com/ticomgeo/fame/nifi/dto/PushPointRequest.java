package com.ticomgeo.fame.nifi.dto;

public class PushPointRequest {
    public static PushPointRequest of(String id, String payload) {
        return new PushPointRequest(id, payload);
    }
    public static final String ID_KEY = "PushPointRequest.id";
    public static final String PAYLOAD_KEY = "PushPointRequest.payload";

    private PushPointRequest(String id, String payload) {
        this.id = id;
        this.payload = payload;
    }

    public final String id;
    public final String payload;
}
