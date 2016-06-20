import com.alibaba.fastjson.JSONObject;
import com.emar.kafka.offset.OffsetValue;
import com.emar.kafka.utils.DateUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
    public static void main(String[] args) throws IOException {
//        Path path = Paths.get("E:/ideaProjects/moxingxing/thor/data");

//        String prefix = "test";
//        String suffix = "dat";

//        System.out.println(path.getFileSystem().toString());

//        String pattern = prefix + "*";

//        DirectoryStream.Filter<Path> filter = entry -> Pattern.matches(pattern, entry.toString());

//        System.out.println("\nHas filter applied:");

//        try (DirectoryStream<Path> ds = Files.newDirectoryStream(path, pattern)) {
//            for (Path file : ds) {
//                System.out.println(file.toString());
//            }
//            ds.close();
//        } catch (IOException e) {
//            System.err.println(e);
//        }

//
//        Map<String, String> map = new HashMap<>();
//
//        map.put("a", null);
//
//        String path1 = "i_rtb_bidreq_10_30_20160613.dat";
//        String path2 = "i_rtb_bidreq_9_30_20160613.dat";
//
////        System.out.println(path1.compareTo(path2));
//        String prefix="i_rtb_bidreq_";
//        String suffix=".dat";
//        String time1 = path1.substring(prefix.length()).split(suffix)[0];
//        String time2 = path2.substring(prefix.length()).split(suffix)[0];
//
//        System.out.println(time1);


        OffsetValue value = new OffsetValue("test1.log", 12L, LocalDateTime.now());


        Map<String, Object> offset = new HashMap<>();
        offset.put("0", value.toString());
        System.out.println(offset);

        String offsetValue = (String) offset.get("0");

        System.out.println(offsetValue);

        OffsetValue jsonObject = JSONObject.parseObject(offsetValue, OffsetValue.class);
        System.out.println(jsonObject.getFile());
        System.out.println(jsonObject.getPosition());
        System.out.println(jsonObject.getLastModifyTime());
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
