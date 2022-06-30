package com.baidu.ai.common.engine.presistent.log.meta;

import com.baidu.ai.common.engine.presistent.log.util.BytesUtil;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class TopicMetaData {

    public static final int FILE_NAME_LENGTH_LIMIT = 128;

    public static final int META_BYTES_LENGTH = FILE_NAME_LENGTH_LIMIT + Integer.BYTES * 3;

    private final char[] fileName = new char[FILE_NAME_LENGTH_LIMIT];

    private final int startRecordId;

    private int endRecordId;

    private int contentBytesLength;

    public TopicMetaData(@NotNull String rawFileName, @NotNull int startRecordId) {
        if (rawFileName.length() == 0 || rawFileName.length() > FILE_NAME_LENGTH_LIMIT) {
            throw new IllegalArgumentException("fileName:" + fileName + " is no more than 128");
        }

        int padding = FILE_NAME_LENGTH_LIMIT - rawFileName.length();
        char[] originChars = rawFileName.toCharArray();
        System.arraycopy(originChars, 0, fileName, padding, originChars.length);

        this.startRecordId = startRecordId;
    }

    protected TopicMetaData(char[] rawFileName, int startRecordId, int endRecordId, int contentBytesLength) {
        System.arraycopy(rawFileName, 0, this.fileName, 0, rawFileName.length);
        this.startRecordId = startRecordId;
        this.endRecordId = endRecordId;
        this.contentBytesLength = contentBytesLength;
    }

    public static TopicMetaData convertByteArrayToMeta(byte[] bytes) {
        byte[] tmpFileNameBytes = new byte[FILE_NAME_LENGTH_LIMIT];
        byte[] tmpIntBytes = new byte[Integer.BYTES];

        int position = 0;
        System.arraycopy(bytes, position, tmpFileNameBytes, 0, FILE_NAME_LENGTH_LIMIT);
        char[] fileName = BytesUtil.convertByteArrayToCharArray(tmpFileNameBytes);

        position += FILE_NAME_LENGTH_LIMIT;
        System.arraycopy(bytes, position, tmpIntBytes, 0, Integer.BYTES);
        int startRecordId = BytesUtil.convertByteArrayToInt(tmpIntBytes);

        position += Integer.BYTES;
        System.arraycopy(bytes, position, tmpIntBytes, 0, Integer.BYTES);
        int endRecordId = BytesUtil.convertByteArrayToInt(tmpIntBytes);

        position += Integer.BYTES;
        System.arraycopy(bytes, position, tmpIntBytes, 0, Integer.BYTES);
        int contentBytesLength = BytesUtil.convertByteArrayToInt(tmpIntBytes);

        TopicMetaData topicMetaData = new TopicMetaData(fileName, startRecordId,
                endRecordId, contentBytesLength);

        return topicMetaData;
    }

    public byte[] convertToByteArray() {
        ByteBuffer buffer = ByteBuffer.allocate(META_BYTES_LENGTH);

        buffer.put(BytesUtil.convertCharArrayToByteArray(fileName));
        buffer.put(BytesUtil.convertIntToByteArray(startRecordId));
        buffer.put(BytesUtil.convertIntToByteArray(endRecordId));
        buffer.put(BytesUtil.convertIntToByteArray(contentBytesLength));

        return buffer.array();
    }

    public void updateEndRecordId(int endRecordId) {
        this.endRecordId = endRecordId;
    }

    public String getFileName() {
        String name = new String(fileName);
        return name.trim();
    }

    public int getStartRecordId() {
        return startRecordId;
    }

    public int getEndRecordId() {
        return endRecordId;
    }

    public int addBytesLength(int tmpBytesLength) {
        contentBytesLength += tmpBytesLength;
        return contentBytesLength;
    }

    public int getContentBytesLength() {
        return contentBytesLength;
    }
}
