import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

/**
 * @author moxingxing
 * @Date 2016/6/12
 */
public class FileChannelTest {
    public static void main(String[] args) throws IOException {
        Path path = Paths.get("E:/ideaProjects/moxingxing/thor/data", "test_5.dat");

        System.out.println(Integer.MAX_VALUE);
        FileChannel fileChannel = FileChannel.open(path, EnumSet.of(StandardOpenOption.READ));

        System.out.println(fileChannel.size());
    }
}
