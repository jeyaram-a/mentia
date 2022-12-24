package org.jhouse.mentia.server;

import org.jhouse.mentia.server.command.*;
import org.jhouse.mentia.store.Store;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.Callable;

public class ConnectionHandler {
    private Map<String, Store> serverStore;

    private CommandParser parser;

    private Socket socket;

    ConnectionHandler(Socket socket, Map<String, Store> serverStore) {
        this.socket = socket;
        this.serverStore = serverStore;
    }


    void handle() {
        try (InputStream in = socket.getInputStream()) {
            Command command = parser.parse(in);
            Runnable run = () -> {
                try {
                    switch (command) {
                        case GetCommand getCommand -> {
                            byte[] val = getCommand.execute();
                        }
                        case PutCommand putCommand -> {
                            putCommand.execute();
                        }

                        case OpenCommand openCommand -> {
                            openCommand.execute();
                        }
                        default -> throw new IllegalStateException("Unexpected command: " + command.getCommandType());
                    }
                } catch (Exception e) {

                }
            };
            // TODO use pool
            Thread.ofVirtual().start(run);
        } catch (Exception e) {
            socket.close();
        }

    }
}
