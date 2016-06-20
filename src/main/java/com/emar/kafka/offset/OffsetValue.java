package com.emar.kafka.offset;

import com.emar.kafka.utils.DateUtils;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * @Author moxingxing
 * @Date 2016/6/20
 */
public class OffsetValue implements Serializable{
    private String file;
    private long position;
    private String lastModifyTime;

    public OffsetValue() {
    }

    public OffsetValue(String file, long position, String lastModifyTime) {
        this.file = file;
        this.position = position;
        this.lastModifyTime = lastModifyTime;
    }

    public OffsetValue(String file, long position, LocalDateTime lastModifyTime) {
        this.file = file;
        this.position = position;
        this.lastModifyTime = DateUtils.getOffsetLastModifyTime(lastModifyTime);
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public String getLastModifyTime() {
        return lastModifyTime;
    }

    public void setLastModifyTime(String lastModifyTime) {
        this.lastModifyTime = lastModifyTime;
    }

    @Override
    public String toString() {
        return "{" +
                    "file:'" + file + '\'' + ", " +
                    "position:" + position + ", " +
                    "lastModifyTime:'" + lastModifyTime + "\'" +
                '}';
    }
}
