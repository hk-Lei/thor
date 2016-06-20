import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author moxingxing
 * @Date 2016/6/20
 */
public class Log4jTest {

    public static Logger LOG = LoggerFactory.getLogger(Log4jTest.class);
    public static void main(String[] args) {
        LOG.error("this is error log");
        LOG.warn("this is warn log");
        LOG.info("this is info log");
        LOG.trace("this is trace log");
    }
}
