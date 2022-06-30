package com.baidu.ai.common.engine.presistent.log;

import com.baidu.ai.common.engine.presistent.log.meta.TopicMetaData;
import com.baidu.ai.common.engine.presistent.log.meta.TopicMetaSummary;
import com.baidu.ai.common.engine.presistent.log.record.FileRecords;
import com.baidu.ai.common.engine.presistent.log.record.Record;
import org.jetbrains.annotations.NotNull;

import java.io.File;

public class UnifiedLog implements AutoCloseable {

    private final String filePath;

    private final String topicName;

    private final TopicMetaSummary topicMetaSummary;

    private FileRecords readFileRecords;

    private FileRecords writeFileRecords;

    private UnifiedLogIterator iterator;

    public UnifiedLog(@NotNull String filePath, @NotNull String topicName) throws Exception {
        this.filePath = filePath + File.separator + topicName + File.separator;
        File dir = new File(this.filePath);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        this.topicName = topicName;

        File metaFile = generateTopicMetaFile();
        this.topicMetaSummary = new TopicMetaSummary(topicName, metaFile);
    }

    public UnifiedLogIterator getIterator() {
        if (iterator == null) {
            synchronized (this) {
                if (iterator == null) {
                    iterator = new UnifiedLogIterator(this);
                }
            }
        }
        return iterator;
    }

    public boolean isReadEnd() throws Exception {
        rollingReader();
        return readFileRecords == null;
    }

    public Record makeNextRecord() {
        return readFileRecords.iterator().next();
    }

    protected void rollingReader() throws Exception {
        TopicMetaData readMeta = null;
        if (readFileRecords == null) {
            readMeta = topicMetaSummary.getCurrentReadMeta();

            if (readMeta == null) {
                return;
            }

            createReadFileRecords(readMeta);
        }

        if (topicMetaSummary.mayReadRolling(readFileRecords)) {
            readFileRecords.close();
            readMeta = topicMetaSummary.readRolling();

            if (readMeta == null) {
                readFileRecords = null;
                return;
            }

            createReadFileRecords(readMeta);
            readFileRecords.resetMetaIndex(0);
        }
    }

    private void createReadFileRecords(TopicMetaData readMeta) throws Exception {
        if (writeFileRecords != null
                && writeFileRecords.isEqualWithFilename(readMeta.getFileName())) {
            readFileRecords = writeFileRecords;
            return;
        }
        readFileRecords = getFileRecordsWithMeta(readMeta);
    }

    public int appendOne(@NotNull final byte[] bytes) throws Exception {
        Record record = generateRecord(bytes);
        rollingWriter(record);

        int appendSize = writeFileRecords.appendOne(record);
        topicMetaSummary.updateWriteMetaInfo(record.getId(), appendSize);
        return appendSize;
    }

    protected void rollingWriter(Record record) throws Exception {
        boolean isNeedRolling = false;

        if (writeFileRecords == null) {
            TopicMetaData writeMeta = topicMetaSummary.getCurrentWriteMeta();
            if (writeMeta != null) {
                writeFileRecords = getFileRecordsWithMeta(writeMeta);
            } else {
                isNeedRolling = true;
            }
        }

        if (!isNeedRolling) {
            isNeedRolling = topicMetaSummary.mayWriteRolling(
                    Integer.BYTES + record.getBytes().length);
        }

        if (isNeedRolling) {
            File newRollingFile = generateNewRecordFile();
            TopicMetaData currentWriteMeta = new TopicMetaData(newRollingFile.getName(), record.getId());
            topicMetaSummary.writeRolling(currentWriteMeta);

            if (writeFileRecords != null) {
                writeFileRecords.close();
            }

            writeFileRecords = new FileRecords(newRollingFile);
        }
    }

    public void resetReadFileRecords(@NotNull final int recordId) throws Exception {
        TopicMetaData metaData = topicMetaSummary.findMeta(recordId);

        if (readFileRecords != null
                && !readFileRecords.isEqualWithFilename(metaData.getFileName())) {
            readFileRecords.close();
            readFileRecords = getFileRecordsWithMeta(metaData);
        } else if (readFileRecords == null) {
            readFileRecords = getFileRecordsWithMeta(metaData);
        }

        int index = recordId - metaData.getStartRecordId();
        readFileRecords.resetMetaIndex(index);
    }

    private Record generateRecord(byte[] bytes) {
        int recordId = topicMetaSummary.generateNewRecordId();
        Record record = new Record(recordId, bytes);
        return record;
    }

    private File generateTopicMetaFile() {
        String name = filePath + "Kubrick" + TopicMetaSummary.META_SUFFIX;
        return new File(name);
    }

    private File generateNewRecordFile() {
        TopicMetaData metaData = topicMetaSummary.getCurrentWriteMeta();
        int currentId = 0;
        if (metaData != null) {
            String oldFileName = metaData.getFileName();
            currentId = Integer.valueOf(oldFileName.substring(0,
                    oldFileName.length() - "_Kubrick.log".length())) + 1;
        }

        String name = filePath + String.format("%04d", currentId) + "_Kubrick.log";
        return new File(name);
    }

    private FileRecords getFileRecordsWithMeta(TopicMetaData metaData) throws Exception {
        File recordFile = new File(filePath + metaData.getFileName());
        FileRecords fileRecords = new FileRecords(recordFile);
        return fileRecords;
    }

    @Override
    public void close() throws Exception {
        topicMetaSummary.close();

        if (readFileRecords != null) {
            readFileRecords.close();
        }

        if (writeFileRecords != null) {
            writeFileRecords.close();
        }
    }

    public String getTopicName() {
        return topicName;
    }
}
