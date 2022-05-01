package org.apache.rocketmq.apis.exception;

public class InternalException extends ClientException{
    public InternalException(String message, Throwable cause) {
        super(message, cause);
    }

    public InternalException(String message) {
        super(message);
    }

    public InternalException(Throwable t) {
        super(t);
    }
}
