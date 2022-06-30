package io.github.kubrickLiu.presistent.log.record;

import java.util.Iterator;

public class RecordsIterator implements Iterator<Record> {

    private final FileRecords fileRecords;

    protected RecordsIterator(FileRecords fileRecords) {
        this.fileRecords = fileRecords;
    }

    @Override
    public boolean hasNext() {
        return !fileRecords.isReadEnd();
    }

    @Override
    public Record next() {
        return fileRecords.makeNext();
    }

    public void reset(int newIndex) {
        fileRecords.resetMetaIndex(newIndex);
    }
}
