import com.emar.kafka.connect.FileStreamSourceTask;

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

/**
 * @Author moxingxing
 * @Date 2016/6/3
 */
public class Test {
    public static String fileRoundUnit = "hour";
    public static DateTimeFormatter format;
    public static String currentTime;
    public static void main(String[] args) {

        Path path = Paths.get("D:\\ideaProjects\\moxingxing\\thor\\data\\test");

        String file = "test";


//        Files.find()
        System.out.println(Files.exists(path));

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
