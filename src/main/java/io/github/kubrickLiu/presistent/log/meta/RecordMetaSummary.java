package io.github.kubrickLiu.presistent.log.meta;


import io.github.kubrickLiu.presistent.log.util.BytesUtil;
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
 * readMetaOffset --- 占用 4 B
 * writeMetaOffset --- 占用 4 B
 * writeRecordOffset --- 占用 4 B
 * meta data .....
 * .......
 */
public class RecordMetaSummary extends AbstractMetaSummary {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordMetaSummary.class);

    public static final String META_SUFFIX = ".meta";

    private static final int BASE_OFFSET = 3 * Integer.BYTES;

    private AtomicInteger readMetaOffset = new AtomicInteger(BASE_OFFSET);

    private AtomicInteger writeMetaOffset = new AtomicInteger(BASE_OFFSET);

    private AtomicInteger writeRecordOffset = new AtomicInteger(0);

    private List<RecordMetaData> metaList = new ArrayList<>();

    public RecordMetaSummary(@NotNull final File metaFile) throws Exception {
        super(metaFile);
        open();
    }

    public boolean add(@NotNull RecordMetaData metaData) {
        byte[] metaBytes = metaData.convertToByteArray();

        try {
            int position = writeMetaOffset.get();
            int appendSize = fileChannel.write(ByteBuffer.wrap(metaBytes), position);

            writeMetaOffset.getAndAdd(appendSize);
            writeRecordOffset.getAndAdd(metaData.getHeadBytesSize() + metaData.getBodyBytesSize());
            metaList.add(metaData);
        } catch (IOException e) {
            LOGGER.warn("file channel write meta data error.", e);
            return false;
        }

        return true;
    }

    public boolean isReadEnd() {
        return getCurrentReadIndex() >= size();
    }

    public RecordMetaData orderGet() {
        int currentIndex = getCurrentReadIndex();
        RecordMetaData metaData = metaList.get(currentIndex);
        readMetaOffset.getAndAdd(RecordMetaData.META_BYTES_LENGTH);
        return metaData;
    }

    public int getCurrentReadIndex() {
        int index = (readMetaOffset.get() - BASE_OFFSET) / RecordMetaData.META_BYTES_LENGTH;
        return index;
    }

    public void resetReadIndex(int newReadIndex) {
        if (newReadIndex >= size()) {
            throw new RuntimeException("new read index : " + newReadIndex + " is illegal position.");
        }

        int newOffset = newReadIndex * RecordMetaData.META_BYTES_LENGTH + BASE_OFFSET;
        readMetaOffset.getAndSet(newOffset);
    }

    @Override
    public void flush() throws Exception {
        ByteBuffer readOffsetBuffer = ByteBuffer.wrap(BytesUtil.convertIntToByteArray(readMetaOffset.get()));
        fileChannel.write(readOffsetBuffer, 0);

        ByteBuffer writeOffsetBuffer = ByteBuffer.wrap(BytesUtil.convertIntToByteArray(writeMetaOffset.get()));
        fileChannel.write(writeOffsetBuffer, 1 * Integer.BYTES);

        ByteBuffer metaSizeOffsetBuffer = ByteBuffer.wrap(
                BytesUtil.convertIntToByteArray(writeRecordOffset.get()));
        fileChannel.write(metaSizeOffsetBuffer, 2 * Integer.BYTES);
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
        writeRecordOffset.getAndSet(BytesUtil.convertByteArrayToInt(bytes));
        buffer.reset();

        LOGGER.info("recover - readMetaOffset:{}, writeMetaOffset:{}, writeRecordOffset:{}",
                readMetaOffset, writeMetaOffset, writeRecordOffset);
    }

    private void recoverData() throws IOException {
        metaList.clear();

        int offset = BASE_OFFSET;
        byte[] bytes = new byte[RecordMetaData.META_BYTES_LENGTH];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.mark();

        RecordMetaData metaData = null;
        int size = 0;
        while (offset < writeMetaOffset.get()) {
            offset += fileChannel.read(buffer, offset);

            metaData = RecordMetaData.convertByteArrayToMeta(bytes);
            metaList.add(metaData);

            buffer.reset();
            size++;
        }

        LOGGER.info("recover meta data size : {}", size);
    }

    public boolean isEmpty() {
        return metaList.isEmpty();
    }

    /**
     * 获取 record write 指针
     *
     * @return
     */
    public int getWriteRecordOffset() {
        return writeRecordOffset.get();
    }

    public int size() {
        return metaList.size();
    }

    /**
     * 获取 meta 文件大小
     *
     * @return
     */
    public int getMetaFileSize() {
        return writeMetaOffset.get();
    }

    /**
     * 获取 record 文件大小
     *
     * @return
     */
    public int getRecordFileSize() {
        return writeRecordOffset.get();
    }
}
