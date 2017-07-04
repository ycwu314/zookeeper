package com.example.demo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryNTimes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;

/**
 * Created by Administrator on 2017/7/3 0003.
 */
@Slf4j
public class TestSemaphore extends DemoApplicationTests {

    @Value("${zoo.serverList}")
    private String serverList;
    private CuratorFramework client = null;

    private static final String LOCK_PATH = "/curator/semaphore";
    private static final String LOCK_PATH2 = "/curator/semaphore2";

    private static final String SHARED_COUNT_PATH = "/curator/shared_count";

    @Before
    public void init() {
        client = CuratorFrameworkFactory.newClient(serverList, new RetryNTimes(3, 1000));
        // call start() to trigger connection
        client.start();
    }

    @After
    public void after() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void testSemaphoreUsingConvention() throws InterruptedException {
        int maxLeases = 3;
        new Thread(() -> {
            InterProcessSemaphoreV2 semaphoreV2 = new InterProcessSemaphoreV2(client, LOCK_PATH2,
                maxLeases);
            Collection<Lease> leases = null;
            try {
                leases = semaphoreV2.acquire(2);
                log.info("111");
                Thread.sleep(2000L);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                semaphoreV2.returnAll(leases);
            }
        }).start();

        new Thread(() -> {
            InterProcessSemaphoreV2 semaphoreV2 = new InterProcessSemaphoreV2(client, LOCK_PATH2,
                maxLeases);
            Collection<Lease> leases = null;
            try {
                leases = semaphoreV2.acquire(3);
                log.info("222");
                Thread.sleep(1000L);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                semaphoreV2.returnAll(leases);
            }
        }).start();

        Thread.sleep(5000L);

    }

    @Test
    public void testSemaphoreUsingSharedCount() throws Exception {

        int total = 3;
        new Thread(new SemaphoreUsingSharedCountTask(client, 1, total, 3000L)).start();

        Thread.sleep(500L);

        new Thread(new SemaphoreUsingSharedCountTask(client, 3, total, 0)).start();

        Thread.sleep(5000L);
    }

    @AllArgsConstructor
    static class SemaphoreUsingSharedCountTask implements Runnable {

        private CuratorFramework client;
        private int qty;
        private int total;
        private long sleepInMS;

        @Override
        public void run() {
            SharedCount sharedCount = new SharedCount(client, SHARED_COUNT_PATH, total);
            sharedCount.addListener(new SharedCountListener() {
                @Override
                public void countHasChanged(SharedCountReader sharedCount, int newCount)
                    throws Exception {
                    log.info("newCount={}", newCount);
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {

                }
            });
            try {
                sharedCount.start();
            } catch (Exception e) {
                e.printStackTrace();
            }

            InterProcessSemaphoreV2 semaphoreV2 = new InterProcessSemaphoreV2(client, LOCK_PATH,
                sharedCount);
            Collection<Lease> leases = null;
            try {
                leases = semaphoreV2.acquire(qty);
                sharedCount.trySetCount(sharedCount.getVersionedValue(), total - qty);
                log.info("share count[version={}, value={}]",
                    sharedCount.getVersionedValue().getVersion(),
                    sharedCount.getVersionedValue().getValue());

                for (Lease lease : leases) {
                    log.info("lease node name={}", lease.getNodeName());
                }

                Thread.sleep(sleepInMS);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (leases != null) {
                    log.info("return lease count={}", leases.size());
                    semaphoreV2.returnAll(leases);
                }
                try {
                    // release SharedCount
                    sharedCount.setCount(sharedCount.getVersionedValue().getValue() + qty);
                    sharedCount.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
