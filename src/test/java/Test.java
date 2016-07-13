import com.emar.kafka.utils.DateUtils;
import com.emar.kafka.utils.RecursivePath;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author moxingxing
 * @Date 2016/6/28
 */
public class Test {
    private static final AtomicInteger counter = new AtomicInteger(new Random().nextInt());

    private static int toPositive(int number) {
        return number & 0x7fffffff;
    }

    public static void main(String[] args) {
//        for (int i =0 ; i < 100; i++)
//            System.out.println(Test.toPositive(counter.getAndIncrement()) % 30);
//        System.out.println(Test.toPositive(Utils.murmur2("{\"schema\":null,\"payload\":null}".getBytes())) %30);


//        System.out.println("1\r11111\r".lastIndexOf('\r'));
        String path = "D:/test";
        String fileRegex = "test*.txt";

        final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + fileRegex);
        DirectoryStream.Filter<Path> filter = entry -> matcher.matches(entry.getFileName());
        String glob = "glob:**/*.zip";
//        final PathMatcher pm = FileSystems.getDefault().getPathMatcher(fileRegex);
//        if (pm.matches(Paths.get(path));

        Path dir = Paths.get(path);
        RecursivePath walk = new RecursivePath(filter);

        try {
            Files.walkFileTree(dir, walk);
        } catch (IOException e) {
            e.printStackTrace();
        }

        int index;
        if (path.endsWith("/")) {
            index = path.length() - 1;
        } else {
            index = path.length();
        }


        long start = System.nanoTime();
        System.out.println(Integer.MAX_VALUE);

        System.out.println(start);

        int i = 0;
        while (true){
            i++;
            if (i == 50) {
                System.out.println(System.nanoTime() - start);
            }
            if (i == 100) {
                System.out.println(System.nanoTime() - start);
            }
            if (i == 500) {
                System.out.println(System.nanoTime() - start);
            }
            if (i == 1000) {
                System.out.println(System.nanoTime() - start);
            }
            if (i == 5000) {
                System.out.println(System.nanoTime() - start);
                System.exit(1);
            }
        }


//        if (index > 0) {
//            for (Path p : walk.paths) {
//                System.out.println("walks: " + p.toString());
//                String substring = p.toString().substring(index);
//                System.out.println(substring);
//            }
//        }

//        System.out.println(Paths.get(path, "/a/1.txt").toString());
//
//        System.out.println(DateUtils.getFileLastModifyTime(path, "/a/2.txt"));

//
//        try (DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(path), fileRegex)) {
//            for (Path file : ds) {
//                String fileName = file.getFileName().toString();
//                System.out.println(fileName);
//            }
//            ds.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}
