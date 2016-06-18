import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumSet;

/**
 * @author moxingxing
 * @Date 2016/6/12
 */
public class FileChannelTest {

    private static Logger LOG = LoggerFactory.getLogger(FileChannelTest.class);
    private static ByteBuffer buffer;
    private static String fileName;
    private static long position;
    private static int remainning;

    public static void main(String[] args){
        Path path = Paths.get("D:/ideaProjects/moxingxing/thor/data", "test_2.dat");

        try (FileChannel fileChannel = (FileChannel.open(path,
                EnumSet.of(StandardOpenOption.READ)))) {
            buffer = ByteBuffer.allocate(9);
            int read = fileChannel.read(buffer);

            System.out.println("nread: " + read);
            buffer.flip();
            System.out.println("mark: " + buffer.mark());
            System.out.println("position: " + buffer.position());
            System.out.println("limit: " + buffer.limit());
            System.out.println("capacity: " + buffer.capacity());
//
            System.out.println("\n==========================\n");
            try {
                CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
                String content = decoder.decode(buffer).toString();
                System.out.println(content);
                buffer.clear();
            } catch (CharacterCodingException ex) {
                ex.printStackTrace();
            }

            System.out.println("\n==========================\n");

            ArrayList<String> lines = extractRecords(path);

            for (String line : lines) {
                System.out.println(line + "=====" + line.length());
            }

            System.out.println("channel.position: " + fileChannel.position());
            System.out.println("Records Position: " + (fileChannel.position() - remainning));


            System.out.println("mark: " + buffer.mark());
            System.out.println("position: " + buffer.position());
            System.out.println("limit: " + buffer.limit());
            System.out.println("capacity: " + buffer.capacity());

            System.out.println("\n######################################\n");

            read = fileChannel.read(buffer);

            System.out.println("nread: " + read);
            buffer.flip();
            System.out.println("mark: " + buffer.mark());
            System.out.println("position: " + buffer.position());
            System.out.println("limit: " + buffer.limit());
            System.out.println("capacity: " + buffer.capacity());
//
            System.out.println("\n==========================\n");
            try {
                CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
                String content = decoder.decode(buffer).toString();
                System.out.println(content);
                buffer.clear();
            } catch (CharacterCodingException ex) {
                ex.printStackTrace();
            }

            System.out.println("\n==========================\n");

            lines = extractRecords(path);

            for (String line : lines) {
                System.out.println(line + "=====" + line.length());
            }
            System.out.println("channel.position: " + fileChannel.position());
            System.out.println("Records Position: " + (fileChannel.position() - remainning));

            System.out.println("mark: " + buffer.mark());
            System.out.println("position: " + buffer.position());
            System.out.println("limit: " + buffer.limit());
            System.out.println("capacity: " + buffer.capacity());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static ArrayList<String> extractRecords(Path file) {
        ArrayList<String> lines = null;
        int limit = buffer.limit();
        byte[] bytes = buffer.array();
        buffer.clear();

        int from, end = -1;
        for (int i = 0; i < limit; i++) {
            if (bytes[i] == '\n') {
                from = end + 1;
                end = i;
                String line = new String(bytes, from, end - from);
                int _r = line.lastIndexOf('\r');
                if (_r == line.length() -1) {
                    line = line.substring(0, _r);
                }

                if (lines == null) {
                    lines = new ArrayList<>();
                }
                if (line.length() > 0) {
                    lines.add(line);
                }
            }
        }
        remainning = limit - (end + 1);
        System.out.println(" remainning : " + remainning);
        buffer.put(bytes, end + 1, remainning);
//        buffer.compact();
        return lines;
    }
}
