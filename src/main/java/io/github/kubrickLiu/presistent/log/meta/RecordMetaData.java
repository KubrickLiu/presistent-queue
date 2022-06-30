package io.github.kubrickLiu.presistent.log.meta;

import io.github.kubrickLiu.presistent.log.util.BytesUtil;

import java.nio.ByteBuffer;

public class RecordMetaData {

    public static final int META_BYTES_LENGTH = Integer.BYTES * 3;

    private final int startOffset;

    private final int headBytesSize;

    private final int bodyBytesSize;

    public RecordMetaData(int startOffset, int headerBytesSize, int bodyBytesSize) {
        this.startOffset = startOffset;
        this.headBytesSize = headerBytesSize;
        this.bodyBytesSize = bodyBytesSize;
    }

    public static RecordMetaData convertByteArrayToMeta(byte[] bytes) {
        byte[] tmpBytes = new byte[Integer.BYTES];

        System.arraycopy(bytes, 0, tmpBytes, 0, Integer.BYTES);
        int startOffset = BytesUtil.convertByteArrayToInt(tmpBytes);

        System.arraycopy(bytes, Integer.BYTES, tmpBytes, 0, Integer.BYTES);
        int headBytesSize = BytesUtil.convertByteArrayToInt(tmpBytes);

        System.arraycopy(bytes, 2 * Integer.BYTES, tmpBytes, 0, Integer.BYTES);
        int bodyBytesSize = BytesUtil.convertByteArrayToInt(tmpBytes);

        RecordMetaData metaData = new RecordMetaData(startOffset, headBytesSize, bodyBytesSize);
        return metaData;
    }

    public byte[] convertToByteArray() {
        ByteBuffer buffer = ByteBuffer.allocate(META_BYTES_LENGTH);
        buffer.put(BytesUtil.convertIntToByteArray(startOffset));
        buffer.put(BytesUtil.convertIntToByteArray(headBytesSize));
        buffer.put(BytesUtil.convertIntToByteArray(bodyBytesSize));

        return buffer.array();
    }

    public int getStartOffset() {
        return startOffset;
    }

    public int getHeadBytesSize() {
        return headBytesSize;
    }

    public int getBodyBytesSize() {
        return bodyBytesSize;
    }
}
