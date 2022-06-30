package io.github.kubrickLiu.presistent.log.exception;

public class FileChannelFullException extends RuntimeException {

    public FileChannelFullException() {
    }

    public FileChannelFullException(String message) {
        super(message);
    }
}
