package org.apache.rocketmq.apis.exception;

public class InternalException extends ClientException {
    public InternalException(String message, Throwable cause) {
        super(message, cause);
    }

    public InternalException(String message) {
        super(message);
    }

    public InternalException(int responseCode, String message) {
        super(message);
        putMetadata(RESPONSE_CODE_KEY, String.valueOf(responseCode));
    }

    public InternalException(Throwable t) {
        super(t);
    }
}
