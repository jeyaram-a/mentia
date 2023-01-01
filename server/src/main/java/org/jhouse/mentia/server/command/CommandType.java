package org.jhouse.mentia.server.command;

public enum CommandType {
    OPEN(1), GET(2), PUT(3);
    public final int  label;

    private CommandType(int val) {
        this.label = val;
    }

}
