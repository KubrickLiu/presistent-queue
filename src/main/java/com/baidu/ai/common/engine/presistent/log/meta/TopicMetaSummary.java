package com.baidu.ai.common.engine.presistent.log.meta;

import com.baidu.ai.common.engine.presistent.log.exception.InvalidMetaException;
import com.baidu.ai.common.engine.presistent.log.record.FileRecords;
import com.baidu.ai.common.engine.presistent.log.util.BytesUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 文件信息：
 * readMetaOffset --- 占用 8 B
 * writeMetaOffset --- 占用 8 B
 * readRecordId --- 占用 8 B
 * meta data .....
 * .......
 */
public class TopicMetaSummary extends AbstractMetaSummary {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicMetaSummary.class);

    public static final String META_SUFFIX = "_summary.log";

    private static final int BASE_OFFSET = 3 * Integer.BYTES;

    private AtomicInteger readMetaOffset = new AtomicInteger(BASE_OFFSET);

    private AtomicInteger writeMetaOffset = new AtomicInteger(BASE_OFFSET);

    private AtomicInteger maxRecordId = new AtomicInteger(0);

    private List<TopicMetaData> metaList = new ArrayList<>();

    private final String topicName;

    private TopicMetaData currentReadMeta;

    private TopicMetaData currentWriteMeta;

    public TopicMetaSummary(@NotNull String topicName, @NotNull File metaFile) throws Exception {
        super(metaFile);
        this.topicName = topicName;
        open();
    }

    public int generateNewRecordId() {
        return maxRecordId.incrementAndGet();
    }

    public boolean mayWriteRolling(int contentBytesLength) {
        int tmpContentBytesLength = currentWriteMeta.getContentBytesLength() + contentBytesLength;
        if (tmpContentBytesLength >= FileRecords.RECORD_FILE_BYTES_LENGTH_LIMIT) {
            return true;
        }

        return false;
    }

    public void writeRolling(@NotNull TopicMetaData metaData) throws Exception {
        byte[] metaBytes = metaData.convertToByteArray();

        try {
            if (currentWriteMeta != null) {
                flushCurrentWriteMeta();
            }

            int position = writeMetaOffset.get();
            int appendSize = fileChannel.write(ByteBuffer.wrap(metaBytes), position);

            writeMetaOffset.getAndAdd(appendSize);
            metaList.add(metaData);
            currentWriteMeta = metaData;
        } catch (IOException e) {
            LOGGER.warn("file channel write topic meta data error.", e);
            throw e;
        }
    }

    public void updateWriteMetaInfo(int id, int contentBytesLength) {
        currentWriteMeta.updateEndRecordId(id);
        currentWriteMeta.addBytesLength(contentBytesLength);
    }

    public TopicMetaData getCurrentWriteMeta() {
        return currentWriteMeta;
    }

    public boolean mayReadRolling(FileRecords readFileRecords) throws Exception {
        if (readFileRecords == null) {
            throw new IllegalAccessException();
        }

        return readFileRecords.isReadEnd();
    }

    public TopicMetaData readRolling() {
        int index = getCurrentReadIndex() + 1;
        if (index >= size()) {
            return null;
        }

        currentReadMeta = metaList.get(index);
        readMetaOffset.getAndAdd(TopicMetaData.META_BYTES_LENGTH);
        return currentReadMeta;
    }

    public TopicMetaData getCurrentReadMeta() {
        if (currentReadMeta == null && metaList.size() > 0) {
            currentReadMeta = metaList.get(0);
        }
        return currentReadMeta;
    }

    public int getCurrentReadIndex() {
        int index = (readMetaOffset.get() - BASE_OFFSET) / TopicMetaData.META_BYTES_LENGTH;
        return index;
    }

    public TopicMetaData findMeta(int recordId) throws Exception {
        for (int i = 0 ; i < metaList.size() ; i++) {
            TopicMetaData data = metaList.get(i);

            if (recordId < data.getStartRecordId()) {
                throw new InvalidMetaException("can not find meta with record id:" + recordId);
            } else if (recordId > data.getEndRecordId()) {
                continue;
            } else {
                readMetaOffset.getAndSet(BASE_OFFSET + i * TopicMetaData.META_BYTES_LENGTH);
                return data;
            }
        }

        throw new IllegalAccessException();
    }

    public int size() {
        return metaList.size();
    }

    @Override
    public void flush() throws Exception {
        ByteBuffer readOffsetBuffer = ByteBuffer.wrap(BytesUtil.convertIntToByteArray(readMetaOffset.get()));
        fileChannel.write(readOffsetBuffer, 0);

        ByteBuffer writeOffsetBuffer = ByteBuffer.wrap(BytesUtil.convertIntToByteArray(writeMetaOffset.get()));
        fileChannel.write(writeOffsetBuffer, 1 * Integer.BYTES);

        ByteBuffer maxRecordIdOffsetBuffer =
                ByteBuffer.wrap(BytesUtil.convertIntToByteArray(maxRecordId.get()));
        fileChannel.write(maxRecordIdOffsetBuffer, 2 * Integer.BYTES);

        flushCurrentWriteMeta();
    }

    private void flushCurrentWriteMeta() throws Exception {
        if (currentWriteMeta != null) {
            int position = writeMetaOffset.get() - TopicMetaData.META_BYTES_LENGTH
                    + TopicMetaData.FILE_NAME_LENGTH_LIMIT + Integer.BYTES;

            ByteBuffer endRecordIdBuffer =
                    ByteBuffer.wrap(BytesUtil.
                            convertIntToByteArray(currentWriteMeta.getEndRecordId()));
            position += fileChannel.write(endRecordIdBuffer, position);

            ByteBuffer contentBytesLengthBuffer =
                    ByteBuffer.wrap(BytesUtil.
                            convertIntToByteArray(currentWriteMeta.getContentBytesLength()));
            fileChannel.write(contentBytesLengthBuffer, position);
        }
    }

    @Override
    protected void recover() {
        try {
            recoverHead();
        } catch (IOException e) {
            LOGGER.error("recover meta head data error.", e);
            close();
            return;
        }

        try {
            recoverData();
        } catch (IOException e) {
            LOGGER.error("recover meta data error.", e);
            close();
            return;
        }

        if (size() > 0) {
            if (getCurrentReadIndex() < size()) {
                currentReadMeta = metaList.get(getCurrentReadIndex());
            } else if (getCurrentReadIndex() == size()) {
                currentReadMeta = metaList.get(metaList.size() - 1);
            }
            currentWriteMeta = metaList.get(metaList.size() - 1);
        }
    }

    private void recoverHead() throws IOException {
        byte[] bytes = new byte[Integer.BYTES];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.mark();

        fileChannel.read(buffer, 0);
        readMetaOffset.getAndSet(BytesUtil.convertByteArrayToInt(bytes));
        buffer.reset();

        fileChannel.read(buffer, 1 * Integer.BYTES);
        writeMetaOffset.getAndSet(BytesUtil.convertByteArrayToInt(bytes));
        buffer.reset();

        fileChannel.read(buffer, 2 * Integer.BYTES);
        maxRecordId.getAndSet(BytesUtil.convertByteArrayToInt(bytes));
        buffer.reset();
    }

    private void recoverData() throws IOException {
        metaList.clear();

        int offset = BASE_OFFSET;
        byte[] bytes = new byte[TopicMetaData.META_BYTES_LENGTH];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.mark();

        TopicMetaData metaData = null;
        int size = 0;
        while (offset < writeMetaOffset.get()) {
            offset += fileChannel.read(buffer, offset);

            metaData = TopicMetaData.convertByteArrayToMeta(bytes);
            metaList.add(metaData);

            buffer.reset();
            size++;
        }

        LOGGER.info("recover topic meta data size : {}", size);
    }

    public String getTopicName() {
        return topicName;
    }
}
