package com.example.demo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.RetryNTimes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;

/**
 * Created by Administrator on 2017/7/3 0003.
 */
@Slf4j
public class TestSharedLock extends DemoApplicationTests {

    @Value("${zoo.serverList}")
    private String serverList;
    private CuratorFramework client = null;

    private static final String LOCK_PATH = "/curator/semaphore";

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

    /**
     * 2 directories created under LOCK_PATH:
     * locks, leases
     *
     * <pre>
     *     get /curator/semaphore/leases/_c_425e826d-d54e-4838-ba55-cf9ba5ad1b58-lease-0000000003
     * 192.168.100.102
     *
     *
     * </pre>
     */
    @Test
    public void test() throws InterruptedException {

        ExecutorService executorService = Executors.newCachedThreadPool();
        IntStream.range(0, 2).forEach(i -> {
            executorService.submit(new InterProcess(client, i));
        });

        Thread.sleep(10000L);

    }

    @AllArgsConstructor
    static class InterProcess implements Runnable {

        private CuratorFramework client;

        private int i;

        @Override
        public void run() {
            InterProcessSemaphoreMutex semaphoreMutex = new InterProcessSemaphoreMutex(client,
                LOCK_PATH);

            try {
                semaphoreMutex.acquire();
                if (semaphoreMutex.isAcquiredInThisProcess()) {
                    log.info("thread {} got the lock", i);
                }
                Thread.sleep(3000L);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (semaphoreMutex.isAcquiredInThisProcess()) {
                    try {
                        semaphoreMutex.release();
                        log.info("thread {} release the lock", i);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

}
