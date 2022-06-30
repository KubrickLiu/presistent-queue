package io.github.kubrickLiu.presistent.log.meta;

import io.github.kubrickLiu.presistent.log.util.FileUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Timer;
import java.util.TimerTask;

public abstract class AbstractMetaSummary implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMetaSummary.class);

    protected File metaFile;

    protected FileChannel fileChannel;

    private Timer timer;

    private final int schedulePeriodMS = 1000 * 60 * 1; // 1 min

    private boolean isNeedRecover = false;

    public AbstractMetaSummary(@NotNull final File metaFile) throws Exception {
        this.metaFile = metaFile;

        if (!metaFile.exists()) {
            metaFile.createNewFile();
        } else if (!FileUtil.isLegalAccessFile(metaFile)) {
            throw new IllegalAccessException("meta data file: " + metaFile.getPath() + " cannot access");
        } else {
            isNeedRecover = true;
        }
    }

    protected void open() throws Exception {
        try {
            this.fileChannel = FileUtil.openFileChannel(metaFile,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);

            // 如果原来存在则需要恢复数据
            if (isNeedRecover) {
                recover();
            } else {
                init();
            }

            scheduleFlush();
        } catch (Exception e) {
            if (this.fileChannel != null) {
                this.fileChannel.close();
            }
            throw e;
        }
    }

    private void init() {
        try {
            flush();
        } catch (Exception e) {
            LOGGER.error("init flush meta head error.", e);
        }
    }

    private void scheduleFlush() {
        if (timer == null) {
            timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    try {
                        flush();
                    } catch (Exception e) {
                        LOGGER.error("schedule flush meta head error.", e);
                    }
                }
            }, schedulePeriodMS, schedulePeriodMS);
        }
    }

    public abstract void flush() throws Exception;

    /**
     * 恢复磁盘数据至内存
     */
    protected abstract void recover();

    @Override
    public void close() {
        if (FileUtil.isLegalAccessChannel(fileChannel)) {
            try {
                flush();

                fileChannel.close();
            } catch (Exception e) {
                LOGGER.error("close meta file error.", e);
            }
        }
    }
}
