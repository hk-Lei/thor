package com.emar.kafka.offset;

import com.emar.kafka.utils.DateUtils;
import com.emar.kafka.utils.FileUtil;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;

/**
 * @Author moxingxing
 * @Date 2016/6/20
 */
public class OffsetValue {
    private String path;
    private String filename;
    private String inode;
    private long position;
    private String lastModifyTime;

    public OffsetValue() {
    }

    public OffsetValue(String path, String filename, String lastModifyTime) {
        this.path = path;
        this.filename = filename;
        this.lastModifyTime = lastModifyTime;
        this.position = 0L;
        this.inode = FileUtil.getFileInode(path, filename);
    }

    public OffsetValue(String path, String filename, LocalDateTime lastModifyTime) {
        this(path, filename, DateUtils.getOffsetLastModifyTime(lastModifyTime));
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

    public void setLastModifyTime(LocalDateTime lastModifyTime) {
        this.lastModifyTime = DateUtils.getOffsetLastModifyTime(lastModifyTime);
    }

    @Override
    public String toString() {
        Path file = Paths.get(path, filename);
        return "{" +
                    "filename:'" + file.toString() + '\'' + ", " +
                    "inode:'" + inode + '\'' + ", " +
                    "position:" + position + ", " +
                    "lastModifyTime:'" + lastModifyTime + "\'" +
                '}';
    }
}
