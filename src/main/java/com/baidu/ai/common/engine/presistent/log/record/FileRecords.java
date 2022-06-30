package com.baidu.ai.common.engine.presistent.log.record;

import com.baidu.ai.common.engine.presistent.log.exception.FileChannelFullException;
import com.baidu.ai.common.engine.presistent.log.meta.RecordMetaData;
import com.baidu.ai.common.engine.presistent.log.meta.RecordMetaSummary;
import com.baidu.ai.common.engine.presistent.log.util.FileUtil;
import com.baidu.ai.common.engine.presistent.log.util.BytesUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;

/**
 * 文件信息：
 * head (record id) --- 占用 4 B
 * data (record content) --- 占用 ? B
 * head (record id) --- 占用 4 B
 * data (record content) --- 占用 ? B
 * .......
 */
public class FileRecords implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileRecords.class);

    /**
     * 500 MB
     */
    public static final int RECORD_FILE_BYTES_LENGTH_LIMIT = 500 * 1024 * 1024;

    private RecordMetaSummary metaDataSummary;

    private File file;

    private FileChannel fileChannel;

    private RecordsIterator iterator;

    public FileRecords(@NotNull File file) throws Exception {
        if (!file.exists()) {
            file.createNewFile();
        } else if (!FileUtil.isLegalAccessFile(file)) {
            throw new IllegalAccessException("records data file: " + file.getPath() + " cannot access");
        }

        try {
            this.file = file;
            this.fileChannel = FileUtil.openFileChannel(file,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);

            File metaFile = new File(file.getAbsolutePath() + RecordMetaSummary.META_SUFFIX);
            this.metaDataSummary = new RecordMetaSummary(metaFile);
        } catch (Exception e) {
            if (this.fileChannel != null) {
                this.fileChannel.close();
            }
            throw e;
        }
    }

    public int append(@NotNull List<Record> records) throws IOException {
        if (!FileUtil.isLegalAccessChannel(fileChannel)) {
            throw new RuntimeException("file channel is not exists or can not write.");
        }

        int appendSize = 0;
        for (Record record : records) {
            appendSize += realAppendOne(record);
        }
        return appendSize;
    }

    public int appendOne(@NotNull Record record) throws IOException {
        if (!FileUtil.isLegalAccessChannel(fileChannel)) {
            throw new RuntimeException("file channel is not exists or can not write.");
        }
        return realAppendOne(record);
    }

    private int realAppendOne(Record record) throws IOException {
        // Head
        int recordId = record.getId();
        byte[] headBytes = BytesUtil.convertIntToByteArray(recordId);
        int headBytesLength = headBytes.length;

        // Body
        byte[] bodyBytes = record.getBytes();
        int bodyBytesLength = bodyBytes.length;

        int startOffset = metaDataSummary.getWriteRecordOffset();

        if ((startOffset + headBytesLength + bodyBytesLength) > RECORD_FILE_BYTES_LENGTH_LIMIT) {
            throw new FileChannelFullException("file : " + file.getName() + " channel is full.");
        }

        int tmpAppendSize = fileChannelAppend(startOffset, headBytes, bodyBytes);
        if (tmpAppendSize != (headBytesLength + bodyBytesLength)) {
            throw new RuntimeException("append size is not equals with head and body bytes size");
        }

        RecordMetaData metaData = new RecordMetaData(startOffset, headBytesLength, bodyBytesLength);
        metaDataSummary.add(metaData);

        return tmpAppendSize;
    }

    private int fileChannelAppend(int baseOffset, byte[] headBytes, byte[] bodyBytes) throws IOException {
        int size = 0;
        size += fileChannel.write(ByteBuffer.wrap(headBytes), baseOffset);
        size += fileChannel.write(ByteBuffer.wrap(bodyBytes), baseOffset + size);
        return size;
    }

    public Iterator<Record> iterator() {
        if (iterator == null) {
            synchronized (this) {
                if (iterator == null) {
                    iterator = new RecordsIterator(this);
                }
            }
        }
        return iterator;
    }

    public boolean isEqualWithFilename(String filename) {
        return file.getName().equals(filename);
    }

    public boolean isReadEnd() {
        return metaDataSummary.isReadEnd();
    }

    public Record makeNext() {
        if (isReadEnd()) {
            return null;
        }

        RecordMetaData metaData = metaDataSummary.orderGet();
        int offset = metaData.getStartOffset();
        int headLen = metaData.getHeadBytesSize();
        int bodyLen = metaData.getBodyBytesSize();

        byte[] headBytes = new byte[headLen];
        byte[] bodyBytes = new byte[bodyLen];
        try {
            fileChannel.read(ByteBuffer.wrap(headBytes), offset);
            fileChannel.read(ByteBuffer.wrap(bodyBytes), offset + headLen);
        } catch (IOException e) {
            throw new RuntimeException();
        }

        int recordId = BytesUtil.convertByteArrayToInt(headBytes);
        return new Record(recordId, bodyBytes);
    }

    public void resetMetaIndex(int newIndex) {
        metaDataSummary.resetReadIndex(newIndex);
    }

    @Override
    public void close() {
        if (FileUtil.isLegalAccessChannel(fileChannel)) {
            try {
                metaDataSummary.close();
                fileChannel.close();
            } catch (Exception e) {
                LOGGER.error("close record file error.", e);
            }
        }
    }
}
