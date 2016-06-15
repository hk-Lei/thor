import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author moxingxing
 * @Date 2016/6/12
 */
public class NIOTest {
    public static void main(String[] args) {
        Path path = Paths.get("D:/ideaProjects/moxingxing/thor/data");

//        System.out.println(Files.exists(path));

        System.out.println("\nGlob pattern applied:");

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(path, "test_*.dat")) {
            for (Path file : ds) {
                System.out.println(file.getFileName());
            }
        } catch (IOException e) {
            System.err.println(e);
        }
    }
}
