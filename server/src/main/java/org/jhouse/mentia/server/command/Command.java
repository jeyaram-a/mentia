package org.jhouse.mentia.server.command;

import java.util.UUID;

public class Command {
    private UUID id;
    private CommandType type;
    private String store;

    public UUID getId() {
        return id;
    }

    public CommandType getType() {
        return type;
    }

    public String getStore() {
        return store;
    }


}
