package com.baidu.ai.common.engine.presistent.log.record;

public class Record {

    private final int id;

    private final byte[] bytes;

    public Record(final int id, final byte[] bytes) {
        this.id = id;
        this.bytes = bytes;
    }

    public int getId() {
        return id;
    }

    public byte[] getBytes() {
        return bytes;
    }
}
