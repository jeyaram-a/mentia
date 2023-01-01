package org.jhouse.mentia.server;

import org.jhouse.mentia.server.command.*;
import org.jhouse.mentia.server.command.exception.PackageExecutionException;
import org.jhouse.mentia.store.Store;
import org.tinylog.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;

public class ConnectionHandler {

    private static String INVALID_STORE_MSG = "Mentioned store doesn't exists. Create a new store with Open Command";
    private Map<String, Store> serverStore;

    private CommandParser parser;

    private CommandExecutor executor;

    private Socket socket;

    ConnectionHandler(Socket socket, Map<String, Store> serverStore) {
        this.socket = socket;
        this.serverStore = serverStore;
    }


    void handle() {
        try (InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {
            var command = parser.parse(in);
            var store = serverStore.get(command.getStore());
            Runnable run = () -> {
                CommandResponse response = null;
                try {
                    switch (command.getType()) {
                        case OPEN -> {
                            var openedStore = executor.executeOpenCommand(command);
                            serverStore.put(command.getStore(), openedStore);
                            response = new CommandResponse(command.getId(), true, null, null);
                        }
                        case PUT -> {
                            executor.executePutCommand(command, store);
                            response = new CommandResponse(command.getId(), true, null, null);
                        }
                        case GET -> {
                            byte[] val = executor.executeGetCommand(command, store);
                            response = new CommandResponse(command.getId(), true, val, null);
                        }
                    }
                } catch (Exception e) {
                    response = new CommandResponse(command.getId(), false, null, e.getMessage());
                    throw new PackageExecutionException(e);
                } finally {
                    if (response != null) {
                        try {
                            out.write(response.toBytes());
                        } catch (IOException e) {
                            throw new PackageExecutionException(e);
                        }
                    }
                }
            };
            // TODO use pool
            Thread.ofVirtual().start(run);
        } catch (Exception e) {
            Logger.error("Error in processing command ", e);
            try {
                socket.close();
            } catch (IOException ex) {
                Logger.error("Error in closing socket after encountering error in processing", ex);
            }
        }

    }
}
