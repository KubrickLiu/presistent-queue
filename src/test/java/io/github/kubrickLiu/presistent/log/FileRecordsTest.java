package io.github.kubrickLiu.presistent.log;

import io.github.kubrickLiu.presistent.log.meta.RecordMetaSummary;
import io.github.kubrickLiu.presistent.log.record.FileRecords;
import io.github.kubrickLiu.presistent.log.record.Record;
import io.github.kubrickLiu.presistent.log.record.RecordsIterator;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class FileRecordsTest {

    private String filePath = "test.log";

    private File file;

    private File metaFile;

    private AtomicInteger idGenerator = new AtomicInteger(0);

    @Before
    public void before() throws IOException {
        file = new File(filePath);
        metaFile = new File(filePath + RecordMetaSummary.META_SUFFIX);
    }

    @Test
    public void testGenerate() throws Exception {
        file.delete();
        metaFile.delete();

        FileRecords fileRecords = new FileRecords(file);

        Record record = null;
        for (int i = 0; i < 3; i++) {
            int id = idGenerator.getAndIncrement();
            String message = "msg-" + id;
            record = new Record(id, message.getBytes());

            fileRecords.appendOne(record);
        }

        Iterator<Record> iterator = fileRecords.iterator();

        while (iterator.hasNext()) {
            Record result = iterator.next();
            System.out.println(result.getId() + " : " + new String(result.getBytes()));
        }

        fileRecords.close();
    }

    @Test
    public void testRecover() throws Exception {
        try (FileRecords fileRecords = new FileRecords(file)) {
            Iterator<Record> iterator = fileRecords.iterator();
            RecordsIterator recordsIterator = (RecordsIterator) iterator;
            recordsIterator.reset(0);

            while (iterator.hasNext()) {
                Record result = iterator.next();
                System.out.println(result.getId() + " : " + new String(result.getBytes()));
            }
        }
    }
}
