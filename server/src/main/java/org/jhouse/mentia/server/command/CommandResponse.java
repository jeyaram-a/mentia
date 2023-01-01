package org.jhouse.mentia.server.command;

import java.util.UUID;

public class CommandResponse {
    private UUID uuid;
    private boolean isSuccess;

    private byte[] response;

    private String errMsg;

    public byte[] toBytes() {
        return null;
    }

    public CommandResponse(UUID uuid, boolean isSuccess, byte[] response, String errMsg) {
        this.uuid = uuid;
        this.isSuccess = isSuccess;
        this.response = response;
        this.errMsg = errMsg;
    }
}
