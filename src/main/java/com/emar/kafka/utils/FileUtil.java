package com.emar.kafka.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Author: moxingxing
 * Date: 2016/8/12
 */
public class FileUtil {
    private static final Logger LOG = LoggerFactory.getLogger(FileUtil.class);

    /**
     * 获取 File 的 inode 值
     * @param path
     * @return
     */
    public static String getFileInode(Path path){
        if (Files.exists(path)) {
            try {
                Object fileKey = Files.getAttribute(path, "basic:fileKey");
                String s = null;
                if (fileKey != null) {
                    s = fileKey.toString();
                    return s.substring(s.indexOf("ino=") + 4, s.indexOf(")"));
                }
            } catch (IOException e) {
                LOG.error("Couldn't readAttributes from File:{} for FileStreamSourceTask!",
                        path.toString());
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * 获取 File 的 inode 值
     * @param path
     * @param filename
     * @return
     */
    public static String getFileInode(String path, String filename){
        Path file = Paths.get(path, filename);
        return getFileInode(file);
    }

    /**
     * 获取 File 的 inode 值
     * @param absolutePathFile
     * @return
     */
    public static String getFileInode(String absolutePathFile){
        Path file = Paths.get(absolutePathFile);
        return getFileInode(file);
    }

    /**
     * 获取 File 的 Size
     * @param path
     * @return
     */
    public static long getFileSize(Path path){
        if (Files.exists(path)) {
            try {
                return (Long) Files.getAttribute(path, "basic:size");
            } catch (IOException e) {
                LOG.error("Couldn't readAttributes from File:{} for FileStreamSourceTask!",
                        path.toString());
                e.printStackTrace();
            }
        }
        return -1L;
    }

    /**
     * 获取 File 的 Size
     * @param path
     * @param filename
     * @return
     */
    public static long getFileSize(String path, String filename){
        Path file = Paths.get(path, filename);
        return getFileSize(file);
    }

    /**
     * 获取 File 的 Size
     * @param absolutePathFile
     * @return
     */
    public static long getFileSize(String absolutePathFile){
        Path file = Paths.get(absolutePathFile);
        return getFileSize(file);
    }

}
