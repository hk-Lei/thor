import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.utils.Utils;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author moxingxing
 * @Date 2016/6/28
 */
public class Test {
    private static final AtomicInteger counter = new AtomicInteger(new Random().nextInt());

    private static int toPositive(int number) {
        return number & 0x7fffffff;
    }
    public static void main(String[] args) {
//        for (int i =0 ; i < 100; i++)
//            System.out.println(Test.toPositive(counter.getAndIncrement()) % 30);
//        System.out.println(Test.toPositive(Utils.murmur2("{\"schema\":null,\"payload\":null}".getBytes())) %30);


        System.out.println("1\r11111\r".lastIndexOf('\r'));
    }
}
