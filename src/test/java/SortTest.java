import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

/**
 * @Author moxingxing
 * @Date 2016/6/15
 */
public class SortTest {
    private static final String FILE = "file";
    private static final String POSITION = "position";
    private static final String CREATION_TIME = "creation_time";

    public static void main(String[] args) {
        ArrayList<Map<String, Object>> offsets = new ArrayList<>();

        Path path = Paths.get("D:/ideaProjects/moxingxing/thor/data");

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(path, "test_*.dat")) {
            for (Path file : ds) {
                BasicFileAttributes attr = Files.readAttributes(file, BasicFileAttributes.class);
                LocalDateTime creationTime = getLocalDateTime(attr.creationTime());
                Map<String, Object> valueMap = new HashMap<>();
                valueMap.put(FILE, file.getFileName().toString());
                valueMap.put(POSITION, 0L);
                valueMap.put(CREATION_TIME, creationTime);
                offsets.add(valueMap);
            }
        } catch (IOException e) {
            System.err.println(e);
        }
        Map<String, Object> [] maps = new Map[offsets.size()];
        maps = offsets.toArray(maps);
        for (Map<String, Object> map : maps) {
            System.out.println(map);
        }

        System.out.println("##########################");

        sortOffsets(maps);

        for (Map<String, Object> map : maps) {
            System.out.println(map);
        }


    }

    private static LocalDateTime getLocalDateTime(FileTime time) {
        return LocalDateTime.ofInstant(time.toInstant(), ZoneId.of("Asia/Shanghai"));
    }

    private static Map<String, Object>[] sortOffsets(Map<String, Object>[] array){
        if (array.length > 1) {
            Arrays.sort(array, SortTest::compareTo);
        }
        return array;
    }

    private static int compareTo(Map<String, Object> o1, Map<String, Object> o2){
        return ((LocalDateTime)o1.get(CREATION_TIME)).compareTo((LocalDateTime)o2.get(CREATION_TIME));
    }
}
