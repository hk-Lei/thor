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
import java.util.EnumSet;

/**
 * @author moxingxing
 * @Date 2016/6/12
 */
public class FileChannelTest {
    public static void main(String[] args){
        Path path = Paths.get("D:/ideaProjects/moxingxing/thor/data", "test_2.dat");

        try (FileChannel fileChannel = (FileChannel.open(path,
                EnumSet.of(StandardOpenOption.READ)))) {
            ByteBuffer buffer = ByteBuffer.allocate(100);
            buffer = buffer.duplicate();
            int read = fileChannel.read(buffer);
            buffer.flip();
            System.out.println("nread: " + read);
            System.out.println("position: " + buffer.position());
            System.out.println("limit: " + buffer.limit());
            System.out.println("capacity: " + buffer.capacity());

            System.out.println("buffer.get():" );
            System.out.println("buffer.arrayOffset: " + buffer.arrayOffset());

            try {
                CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
                String content = decoder.decode(buffer).toString();
                System.out.println(content);
                buffer.clear();
            } catch (CharacterCodingException ex) {
                ex.printStackTrace();
            }

            System.out.println("==========" + fileChannel.position());
            System.out.println(fileChannel.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
