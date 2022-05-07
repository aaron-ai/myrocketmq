package org.apache.rocketmq.apis.exception;

public class TelemetryException extends ClientException {
    public TelemetryException(String message, Throwable cause) {
        super(message, cause);
    }
}
