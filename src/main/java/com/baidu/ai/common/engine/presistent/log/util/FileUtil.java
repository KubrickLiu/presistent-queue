package com.baidu.ai.common.engine.presistent.log.util;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class FileUtil {

    public static boolean isLegalAccessFile(@NotNull final File file) {
        if (file.isFile() && file.canRead() && file.canWrite()) {
            return true;
        }
        return false;
    }

    public static FileChannel openFileChannel(@NotNull final File file, StandardOpenOption... options)
            throws Exception {
        if (!isLegalAccessFile(file)) {
            throw new IllegalAccessException("file is illegal, cannot access");
        }

        if (options.length == 0) {
            throw new RuntimeException("StandardOpenOptions cannot empty");
        }

        if (options.length > 3) {
            throw new RuntimeException("StandardOpenOptions size no more than 3");
        }

        FileChannel channel = null;

        switch (options.length) {
            case 1:
                channel = FileChannel.open(file.toPath(), options[0]);
                break;

            case 2:
                channel = FileChannel.open(file.toPath(), options[0], options[1]);
                break;

            case 3:
                channel = FileChannel.open(file.toPath(), options[0], options[1], options[2]);
                break;

            default:
                throw new IllegalAccessException();
        }

        return channel;
    }

    public static boolean isLegalAccessChannel(@NotNull final FileChannel fileChannel) {
        if (fileChannel.isOpen()) {
            return true;
        }
        return false;
    }
}
