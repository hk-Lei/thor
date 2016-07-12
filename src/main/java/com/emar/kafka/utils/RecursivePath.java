package com.emar.kafka.utils;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;

/**
 * Author: moxingxing
 * Date: 2016/7/12
 */
public class RecursivePath extends SimpleFileVisitor<Path> {

    public Set<Path> paths;
    public DirectoryStream.Filter<Path> filter;

    public RecursivePath(DirectoryStream.Filter<Path> filter) {
        this.paths = new HashSet<>();
        this.filter = filter;
    }

    public void clear(){
        this.paths = new HashSet<>();
    }


    @Override
    public FileVisitResult preVisitDirectory(Path file,
                                             BasicFileAttributes attrs) throws IOException {
        DirectoryStream<Path> stream = Files.newDirectoryStream(file, this.filter);
        for (Path path : stream) {
            this.paths.add(path);
        }
        return FileVisitResult.CONTINUE;
    }
}
