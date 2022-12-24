package org.jhouse.mentia.server.command;

import org.jhouse.mentia.store.Store;

import java.net.Socket;
import java.util.UUID;

public abstract class Command {

    private UUID uuid;
    private String storeName;

    private String commandType;

    public String getStoreName() {
        return storeName;
    }

    public String getCommandType() {
        return commandType;
    }

    public UUID getUuid() {
        return uuid;
    }
}
