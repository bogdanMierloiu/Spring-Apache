package com.bogdanmierloiu.dto;

public class LogsAverageResponse {

    public LogsAverageResponse(String duration, String endpoint) {
        this.duration = duration;
        this.endpoint = endpoint;
    }

    private String duration;

    private String endpoint;

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }
}
