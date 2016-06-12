import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @Author moxingxing
 * @Date 2016/6/3
 */
public class Test {
    public static String fileRoundUnit = "hour";
    public static DateTimeFormatter format;
    public static String currentTime;
    public static void main(String[] args) throws IOException {
        Path path = Paths.get("E:/ideaProjects/moxingxing/thor/data");

        String prefix = "test";
        String suffix = ".dat";

//        System.out.println(path.getFileSystem().toString());

        String pattern = prefix + "*";

//        DirectoryStream.Filter<Path> filter = entry -> Pattern.matches(pattern, entry.toString());

        System.out.println("\nHas filter applied:");

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(path, pattern)) {
            for (Path file : ds) {
                System.out.println(file.toString());
            }
            ds.close();
        } catch (IOException e) {
            System.err.println(e);
        }

    }

    public static String getNextTime(){
        switch (fileRoundUnit) {
            case "minute" :
                return LocalDateTime.parse(currentTime, format).plusMinutes(10).format(format);
            case "hour" :
                return LocalDateTime.parse(currentTime, format).plusHours(1).format(format);
            case "day" :
                return LocalDateTime.parse(currentTime, format).plusDays(1).format(format);
            default :
                return null;
        }
    }
}
