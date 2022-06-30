package com.baidu.ai.common.engine.presistent.log;

import com.baidu.ai.common.engine.presistent.log.record.Record;

import java.util.Iterator;

public class InvalidIterator implements Iterator<Record> {

    public static final InvalidIterator INSTANCE = new InvalidIterator();

    private InvalidIterator() {}

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Record next() {
        return null;
    }
}
