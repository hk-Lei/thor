import com.emar.kafka.offset.OffsetValue;
import com.emar.kafka.utils.DateUtils;
import com.emar.kafka.utils.ListTreeDir;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
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
        String path="D:/test";
        String fileRegex="test_*.txt";

        String glob = "glob:**/*.zip";
//        final PathMatcher pm = FileSystems.getDefault().getPathMatcher(fileRegex);
//        if (pm.matches(Paths.get(path));

        Path dir = Paths.get(path);
        ListTreeDir walk = new ListTreeDir();

        try {
            Files.walkFileTree(dir, walk);
        } catch (IOException e) {
            e.printStackTrace();
        }


        System.out.println(Paths.get(path, "/a/test_1.txt"));

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
