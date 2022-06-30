package io.github.kubrickLiu.presistent.log;

import io.github.kubrickLiu.presistent.log.record.Record;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;

public class UnifiedLogTest {

    private String filePath = "/tmp/kubrik_test_log";

    private String topicName = "test_topic";

    @Test
    public void testGenerate() throws Exception {
        File dir = new File(filePath);
        if (dir.exists()) {
            deleteDir(dir);
            dir.delete();
        }

        UnifiedLog unifiedLog = new UnifiedLog(filePath, topicName);

        for (int i = 0; i < 10; i++) {
            String message = "msg-" + i;
            unifiedLog.appendOne(message.getBytes());
        }

        Iterator<Record> iterator = unifiedLog.getIterator();
        while(iterator.hasNext()) {
            Record record = iterator.next();
            System.out.println(new String(record.getBytes()));
        }

        unifiedLog.close();
    }

    private void deleteDir(File dir) {
        File[] files = dir.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                deleteDir(file);
            } else {
                file.delete();
            }
        }
    }

    @Test
    public void testRecover() throws Exception {
        try (UnifiedLog unifiedLog = new UnifiedLog(filePath, topicName)) {
            Iterator<Record> iterator = unifiedLog.getIterator();
            ((UnifiedLogIterator) iterator).reset(8);

            while(iterator.hasNext()) {
                Record record = iterator.next();
                System.out.println(new String(record.getBytes()));
            }
        }
    }
}
