package com.nus.cool.queryserver.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class NodeInfo {

    // worker status can be free, busy
    public enum Status { FREE, BUSY }

    private String ip;

    private Status status;

    public NodeInfo() {}

    public NodeInfo(String ip, Status status) {
        this.ip = ip;
        this.status = status;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public byte[] toByteArray() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String content = mapper.writeValueAsString(this);
        return content.getBytes();
    }

    public static NodeInfo read(byte[] bytes) throws IOException {
        String content = new String(bytes);
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(content, NodeInfo.class);
    }
}
