package org.jhouse.mentia.server.command.exception;

public class PackageExecutionException extends RuntimeException {

    public PackageExecutionException(String msg, Exception e) {
        super(msg, e);
    }

    public PackageExecutionException( Exception e) {
        super(e);
    }
}
