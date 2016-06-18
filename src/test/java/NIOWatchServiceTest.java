import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;

/**
 * @author moxingxing
 * @Date 2016/6/12
 */
public class NIOWatchServiceTest {
    public static void main(String[] args) {
        Path path = Paths.get("E:/ideaProjects/moxingxing/thor/data1");

        try {
            WatchService watchService = FileSystems.getDefault().newWatchService();

            path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_DELETE);

            while (true) {
                final WatchKey key = watchService.poll();

                if (key != null) {
                    watchService.close();

//                    System.out.println(key);
                    for (WatchEvent<?> watchEvent : key.pollEvents()) {
                        final WatchEvent<Path> watchEventPath = (WatchEvent<Path>) watchEvent;
                        final Path filename = watchEventPath.context();
                        System.out.println(filename);
                    }

                    watchService = FileSystems.getDefault().newWatchService();
                    path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_DELETE);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
