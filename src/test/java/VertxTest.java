import io.vertx.core.Vertx;

/**
 * @Author moxingxing
 * @Date 2016/6/21
 */
public class VertxTest {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();

        vertx.setTimer(2000,
                id -> System.out.println("Timer fired")
        );
    }
}
