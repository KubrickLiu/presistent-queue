package com.baidu.ai.common.engine.presistent.log.exception;

public class FileChannelFullException extends RuntimeException {

    public FileChannelFullException() {
    }

    public FileChannelFullException(String message) {
        super(message);
    }
}
