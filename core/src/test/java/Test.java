import com.emar.kafka.connect.FileStreamSourceTask;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * @Author moxingxing
 * @Date 2016/6/3
 */
public class Test {
    public static String fileRoundUnit = "hour";
    public static DateTimeFormatter format;
    public static String currentTime;
    public static void main(String[] args) throws IOException {
        Path path = Paths.get("D:\\ideaProjects\\moxingxing\\thor\\data");

        String prefix = "test_";
        String suffix = ".dat";

        System.out.println(path.getFileSystem().toString());

        String pattern = prefix + "*" + suffix;

//        DirectoryStream.Filter<Path> filter = entry -> Pattern.matches(pattern, entry.toString());

        System.out.println("\nHas filter applied:");

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(path)) {
            for (Path file : ds) {
                Path fileName = file.getFileName();
                if (fileName.toString().startsWith(prefix) && fileName.toString().endsWith(suffix)){
                    System.out.println(path + File.separator + fileName);
                }
            }
        }catch(IOException e) {
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

    public static String escapeExprSpecialWord(String keyword) {
        if (StringUtils.isNotBlank(keyword)) {
            String[] fbsArr = { "\\", "$", "(", ")", "+", "[", "]", "?", "^", "{", "}", "|" };
            for (String key : fbsArr) {
                if (keyword.contains(key)) {
                    keyword = keyword.replace(key, "\\" + key);
                }
            }
        }
        return keyword;
    }
}
