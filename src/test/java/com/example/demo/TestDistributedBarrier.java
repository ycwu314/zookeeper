package com.example.demo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.RetryNTimes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;

/**
 * Created by Administrator on 2017/7/4 0004.
 */
@Slf4j
public class TestDistributedBarrier extends DemoApplicationTests {

    @Value("${zoo.serverList}")
    private String serverList;
    private CuratorFramework client = null;


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
    public void testBarrier() throws Exception {
        final String BARRIER_PATH = "/curator/barrier";

        final int THREAD_COUNT = 3;
        AtomicInteger finishedCounter = new AtomicInteger(0);

        DistributedBarrier barrier = new DistributedBarrier(client, BARRIER_PATH);
        log.info("##### set the barrier");
        try {
            barrier.setBarrier();
        } catch (Exception e) {
            e.printStackTrace();
        }

        ExecutorService executorService = Executors.newCachedThreadPool();

        IntStream.range(0, THREAD_COUNT).forEach(i -> {
            executorService.submit(() -> {
                DistributedBarrier b = new DistributedBarrier(client, BARRIER_PATH);
                log.info("thread #{} waits on barrier", i);
                try {
                    b.waitOnBarrier();
                    log.info("entered barrier");
                    finishedCounter.incrementAndGet();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        });

        Thread.sleep(1000L);

        barrier.removeBarrier();
        log.info("removed barrier");

        while (finishedCounter.get() != THREAD_COUNT) {
        }

        executorService.shutdown();
    }

    @Test
    public void testDoubleBarrier() throws InterruptedException {
        final String BARRIER_PATH = "/curator/double_barrier";
        int memberQty = 3;

        AtomicInteger counter = new AtomicInteger(0);

        ExecutorService executorService = Executors.newCachedThreadPool();
        IntStream.range(0, memberQty).forEach(i -> {
            executorService.submit(() -> {
                DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(client,
                    BARRIER_PATH,
                    memberQty);

                try {
                    log.info("thread #{} try to enter barrier", i);
                    barrier.enter();
                    log.info("thread #{} does sth", i);
                    barrier.leave();
                    counter.incrementAndGet();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        });

        Thread.sleep(1000L);

        while (counter.get() != memberQty) {
        }

        log.info("exit");
    }

}
