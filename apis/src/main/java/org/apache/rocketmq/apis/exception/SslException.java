package org.apache.rocketmq.apis.exception;

public class SslException extends ClientException{
    public SslException(ErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }

    public SslException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }
}
