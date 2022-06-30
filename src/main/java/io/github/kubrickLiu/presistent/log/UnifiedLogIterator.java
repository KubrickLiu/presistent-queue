package io.github.kubrickLiu.presistent.log;

import io.github.kubrickLiu.presistent.log.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class UnifiedLogIterator implements Iterator<Record> {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnifiedLogIterator.class);

    private final UnifiedLog unifiedLog;

    public UnifiedLogIterator(UnifiedLog unifiedLog) {
        this.unifiedLog = unifiedLog;
    }

    @Override
    public boolean hasNext() {
        try {
            return !unifiedLog.isReadEnd();
        } catch (Exception e) {
            LOGGER.error("unifiedLog has next error.", e);
            return false;
        }
    }

    @Override
    public Record next() {
        Record record = unifiedLog.makeNextRecord();
        return record;
    }

    public void reset(int recordId) throws Exception {
        unifiedLog.resetReadFileRecords(recordId);
    }
}
