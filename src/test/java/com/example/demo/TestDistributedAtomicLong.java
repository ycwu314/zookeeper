package com.example.demo;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.atomic.PromotedToLock;
import org.apache.curator.retry.RetryNTimes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;

/**
 * Created by Administrator on 2017/7/5 0005.
 */
@Slf4j
public class TestDistributedAtomicLong extends DemoApplicationTests {

    @Value("${zoo.serverList}")
    private String serverList;
    private CuratorFramework client = null;
    private Random random = new Random();

    private static final String ATOMIC_LONG_PATH = "/curator/atomic_long";

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
     * optimistic lock mode, randomize sleep is a good thing
     */
    @Test
    public void testByOptimisticMode() throws Exception {
        final int THREAD_COUNT = 100;

        if (client.checkExists().forPath(ATOMIC_LONG_PATH) != null) {
            client.delete().deletingChildrenIfNeeded().forPath(ATOMIC_LONG_PATH);
        }

        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicInteger successCounter = new AtomicInteger();

        ExecutorService executorService = Executors.newCachedThreadPool();

        IntStream.range(0, THREAD_COUNT).forEach(i -> {
            executorService.submit(() -> {
                DistributedAtomicLong atomicLong = new DistributedAtomicLong(client,
                    ATOMIC_LONG_PATH,
                    new RetryNTimes(35, random.nextInt(100)));

                try {
                    countDownLatch.await();
                    AtomicValue<Long> ret = atomicLong.add(1L);
                    if (ret.succeeded()) {
                        successCounter.incrementAndGet();
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        });

        Thread.sleep(3000L);
        countDownLatch.countDown();

        Thread.sleep(3000L);
        DistributedAtomicLong atomicLong = new DistributedAtomicLong(client,
            ATOMIC_LONG_PATH,
            new RetryNTimes(20, random.nextInt(30)));

        log.info("counter={}, actual={}", successCounter.get(),
            atomicLong.get().postValue().longValue());

        executorService.shutdown();
    }


    @Test
    public void testByOptimisticAndMutexMode() throws Exception {
        final int THREAD_COUNT = 100;

        if (client.checkExists().forPath(ATOMIC_LONG_PATH) != null) {
            client.delete().deletingChildrenIfNeeded().forPath(ATOMIC_LONG_PATH);
        }

        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicInteger counter = new AtomicInteger();

        ExecutorService executorService = Executors.newCachedThreadPool();

        IntStream.range(0, THREAD_COUNT).forEach(i -> {
            executorService.submit(() -> {
                PromotedToLock promotedToLock = PromotedToLock.builder()
                    .lockPath(ATOMIC_LONG_PATH + "/lock").retryPolicy(new RetryNTimes(10, 10))
                    .build();
                DistributedAtomicLong atomicLong = new DistributedAtomicLong(client,
                    ATOMIC_LONG_PATH,
                    new RetryNTimes(3, random.nextInt(10)), promotedToLock);

                try {
                    countDownLatch.await();
                    AtomicValue<Long> ret = atomicLong.add(1L);
                    if (ret.succeeded()) {
                        counter.incrementAndGet();
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        });

        Thread.sleep(3000L);
        countDownLatch.countDown();

        Thread.sleep(3000L);
        DistributedAtomicLong atomicLong = new DistributedAtomicLong(client,
            ATOMIC_LONG_PATH,
            new RetryNTimes(3, 10));
        log.info("counter={}, actual={}", counter.get(), atomicLong.get().postValue().longValue());

        executorService.shutdown();
    }


}
