package redis.clients.jedis.test;

import org.databene.contiperf.PerfTest;
import org.databene.contiperf.junit.ContiPerfRule;
import org.databene.contiperf.report.CSVSummaryReportModule;
import org.databene.contiperf.report.HtmlReportModule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.util.SafeEncoder;

import java.io.IOException;
import java.util.Random;

public class GetSetPerformanceTest {
    public static final int DATA_SIZE = 4;

    public static final int TEST_DURATION_MILLIS = 90 * 1000;
    public static final int OPERATIONS_PER_TEST = 1000;
    public static final int WARMUP_MILLIS = 30 * 1000;

    protected static byte[] contents = new byte[DATA_SIZE];
    protected static HostAndPort hnp = new HostAndPort("127.0.0.1", 6379);

    protected Jedis jedis;

    static {
        new Random().nextBytes(contents);
    }

    @Rule
    public ContiPerfRule rule = new ContiPerfRule(
            new HtmlReportModule(),
            new CSVSummaryReportModule());

    public GetSetPerformanceTest() throws IOException {
        super();
    }

    @Before
    public void setUp() throws IOException {
        jedis = new Jedis(hnp.getHost(), hnp.getPort());
        jedis.connect();
        jedis.flushAll();

        System.out.println("preparing complete, start running...");
    }

    @Test
    @PerfTest(duration = TEST_DURATION_MILLIS, threads = 1, warmUp = WARMUP_MILLIS)
    public void testGetSetWithNormalJedis() throws IOException {
        for (int i = 0 ; i < OPERATIONS_PER_TEST / 2 ; i++) {
            byte[] bKey = SafeEncoder.encode("foo");
            jedis.set(bKey, contents);
            jedis.get(bKey);
        }
    }

    @Test
    @PerfTest(duration = TEST_DURATION_MILLIS, threads = 1, warmUp = WARMUP_MILLIS)
    public void testGetSetWithPipelinedJedis() throws IOException {
        Pipeline p = jedis.pipelined();
        byte[] bKey = SafeEncoder.encode("foo");
        for (int i = 0 ; i < OPERATIONS_PER_TEST / 2 ; i++) {
            p.set(bKey, contents);
            p.get(bKey);
        }
        p.sync();
    }

}
