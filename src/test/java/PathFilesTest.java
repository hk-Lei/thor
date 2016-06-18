import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * @author moxingxing
 * @Date 2016/6/12
 */
public class PathFilesTest {
    public static void main(String[] args) throws IOException {
        Path path = Paths.get("D:/ideaProjects/moxingxing/thor/data");

        BasicFileAttributes attr = null;

        try {
            attr = Files.readAttributes(path, BasicFileAttributes.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(attr.creationTime());
        LocalDateTime time = LocalDateTime.ofInstant(attr.creationTime().toInstant(), ZoneId.of("Asia/Shanghai"));
        System.out.println(attr.lastAccessTime());
        System.out.println(time.compareTo(LocalDateTime.ofInstant(attr.lastAccessTime().toInstant(), ZoneId.of("Asia/Shanghai"))));
    }
}
