package redis.clients.jedis.test;

import org.databene.contiperf.PerfTest;
import org.databene.contiperf.junit.ContiPerfRule;
import org.databene.contiperf.report.CSVSummaryReportModule;
import org.databene.contiperf.report.HtmlReportModule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.async.AsyncJedis;
import redis.clients.jedis.async.callback.AsyncResponseCallback;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.SafeEncoder;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class GetSetPerformanceWithAsyncTest extends GetSetPerformanceTest {
    protected AsyncJedis asyncJedis;

    static {
        new Random().nextBytes(contents);
    }

    @Rule
    public ContiPerfRule rule = new ContiPerfRule(
            new HtmlReportModule(),
            new CSVSummaryReportModule());

    public GetSetPerformanceWithAsyncTest() throws IOException {
        super();
    }

    @Before
    public void setUp() throws IOException {
        super.setUp();

        asyncJedis = new AsyncJedis(hnp.getHost(), hnp.getPort());
        System.out.println("preparing complete, start running...");
    }

    @After
    public void tearDown() throws IOException {
        asyncJedis.close();
    }

    @Test
    @PerfTest(duration = TEST_DURATION_MILLIS, threads = 1, warmUp = WARMUP_MILLIS)
    public void testGetSetWithAsyncJedis() throws IOException {
        ResponseCounterCallback<String> setCounterCallback = new ResponseCounterCallback<String>();
        ResponseCounterCallback<byte[]> getCounterCallback = new ResponseCounterCallback<byte[]>();

        byte[] bKey = SafeEncoder.encode("foo");
        for (int i = 0 ; i < OPERATIONS_PER_TEST / 2 ; i++) {
            asyncJedis.set(setCounterCallback, bKey, contents);
            asyncJedis.get(getCounterCallback, bKey);
        }

        while (true) {
            if (setCounterCallback.getCount().get() == OPERATIONS_PER_TEST / 2 &&
                    getCounterCallback.getCount().get() == OPERATIONS_PER_TEST / 2) {
                break;
            }
            try {
                Thread.sleep(0);
            } catch (InterruptedException e) {
                //
            }
        }
    }

    public static class ResponseCounterCallback<T> implements AsyncResponseCallback<T> {
        private volatile AtomicInteger count = new AtomicInteger(0);

        @Override
        public void execute(T response, JedisException exc) {
            if (exc != null) {
                System.err.println("Exception occurred : " + exc);
            } else {
                count.incrementAndGet();
            }
        }

        public AtomicInteger getCount() {
            return count;
        }

    }

}
