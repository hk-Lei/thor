package com.emar.kafka.offset;

import com.emar.kafka.utils.DateUtils;
import com.emar.kafka.utils.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

/**
 * @Author moxingxing
 * @Date 2016/6/20
 */
public class OffsetValue {
    private static final Logger LOG = LoggerFactory.getLogger(OffsetValue.class);
    private String path;
    private String filename;
    private String inode;
    private long position;
    private String lastModifyTime;

    public OffsetValue() {
    }

    public OffsetValue(String path, String filename, long position, String lastModifyTime) {
        this.path = path;
        this.filename = filename;
        this.position = position;
        this.lastModifyTime = lastModifyTime;
        this.inode = FileUtil.getFileInode(path, filename);
    }

    public OffsetValue(String path, String filename, long position, LocalDateTime lastModifyTime) {
        this(path, filename, position, DateUtils.getOffsetLastModifyTime(lastModifyTime));
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getInode() {
        return inode;
    }

    public void setInode(String inode) {
        this.inode = inode;
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
                    "filename:'" + filename + '\'' + ", " +
                    "position:" + position + ", " +
                    "lastModifyTime:'" + lastModifyTime + "\'" +
                '}';
    }
}
