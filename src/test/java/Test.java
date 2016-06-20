import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.deserializer.ExtraProcessor;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.emar.kafka.utils.DateUtils;

import java.io.File;
import java.io.IOException;
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
    public static JSONObject files;
    public static String path = "E:/ideaProjects/moxingxing/thor/data";
    public static String filename = "test_3.dat";
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


//        OffsetValue value = new OffsetValue("test1.log", 12L, LocalDateTime.now());
//
//
//        Map<String, Object> offset = new HashMap<>();
//        offset.put("0", value.toString());
//        System.out.println(offset);
//
//        String offsetValue = (String) offset.get("0");
//
//        System.out.println(offsetValue);
//
//        OffsetValue jsonObject = JSONObject.parseObject(offsetValue, OffsetValue.class);
//        System.out.println(jsonObject.getFile());
//        System.out.println(jsonObject.getPosition());
//        System.out.println(jsonObject.getLastModifyTime());


//        Map<String, Long> offsetMap = new HashMap<>();
//
//        offsetMap.put("test1", 14L);
//        offsetMap.put("test2", 18L);
//
//        String json = JSON.toJSONString(offsetMap);
//        System.out.println("#########################################");
//        System.out.println(json);
//        System.out.println("#########################################");
//
//        ExtraProcessor processor = (object, key, value) -> {
//            System.out.println(key);
//            System.out.println(value);
//            Map<String, Long> vo = (Map<String, Long>) object;
//            vo.put(key, Long.valueOf(value.toString()));
//        };
//
//        Map<String, Long> jsonOffset = JSONObject.parseObject(json, Map.class, processor);
////        FilesStoredOffset jsonOffset = JSONObject.parseObject(json, FilesStoredOffset.class);
//        Long position = jsonOffset.get("test1");
//        System.out.println(position == null ? 0L : position);

        files = new JSONObject();
        files.put("test_1.dat", 100);
        files.put("test_2.dat", 10);
        files.put("test_3.dat", 20);

        removeOlderFromFiles();

        System.out.println(files);

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

    public static void removeOlderFromFiles() {
        if (files == null || files.size() <= 1) {
            return;
        }

        JSONObject object = files;
        files = new JSONObject();
        for (String file : object.keySet()) {
            if (!file.equals(filename)) {
                LocalDateTime fileLastModifyTime = DateUtils.getFileLastModifyTime(path, file);
                if (fileLastModifyTime == null || LocalDateTime.now().minusMinutes(1).compareTo(fileLastModifyTime) > 0) {
                    System.out.printf("remove file: {fileName:%s, position:%d} from files:%s " + File.separator, file, object.get(file), object);
                    continue;
                }
            }
            files.put(file, object.get(file));
        }
//        files.keySet().stream().filter(file -> !file.equals(filename)).forEach(
//                file -> {
//                    LocalDateTime fileLastModifyTime = DateUtils.getFileLastModifyTime(path, file);
//                    if (fileLastModifyTime == null || LocalDateTime.now().minusMinutes(1).compareTo(fileLastModifyTime) > 0) {
//                        LOG.info("remove file: {fileName:{}, position:{}} from files:{}", file, files.get(file), files);
//                        files.remove(file);
//                    }
//                });
    }
}
