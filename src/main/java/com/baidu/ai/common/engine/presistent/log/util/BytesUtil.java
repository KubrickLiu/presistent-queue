package com.baidu.ai.common.engine.presistent.log.util;

public class BytesUtil {

    public static byte[] convertIntToByteArray(int bodyBytesLength) {
        byte[] bytes = new byte[Integer.BYTES];
        int length = bytes.length;
        for (int i = 0; i < length; i++) {
            bytes[length - i - 1] = (byte) (bodyBytesLength & 0xFF);
            bodyBytesLength >>= 8;
        }
        return bytes;
    }

    public static int convertByteArrayToInt(byte[] bytes) {
        int value = 0;
        for (byte b : bytes) {
            value = (value << 8) + (b & 0xFF);
        }
        return value;
    }

    public static byte[] convertCharArrayToByteArray(char[] chars) {
        byte[] bytes = new byte[chars.length];
        for (int i = 0 ; i < chars.length ; i++) {
            bytes[i] = (byte) chars[i];
        }
        return bytes;
    }

    public static char[] convertByteArrayToCharArray(byte[] bytes) {
        char[] chars = new char[bytes.length];
        for (int i = 0 ; i < bytes.length ; i++) {
            chars[i] = (char) bytes[i];
        }
        return chars;
    }
}
