package com.emar.kafka.utils;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.util.HashSet;
import java.util.Set;

/**
 * Author: moxingxing
 * Date: 2016/7/12
 */
public class ListTreeDir extends SimpleFileVisitor<Path> {

//    public
//
//    public ListTreeDir(Set<Path> paths) {
//        paths = new HashSet<>();
//    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
        System.out.println("Visited directory: " + dir.toString());
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) {
        System.out.println(exc);
        return FileVisitResult.CONTINUE;
    }
}
