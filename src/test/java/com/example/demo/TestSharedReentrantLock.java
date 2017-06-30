package com.example.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.RevocationListener;
import org.apache.curator.framework.recipes.locks.Revoker;
import org.apache.curator.retry.RetryNTimes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;

/**
 * Created by Administrator on 2017/6/29 0029.
 */
@Slf4j
public class TestSharedReentrantLock extends DemoApplicationTests {

    @Value("${zoo.serverList}")
    private String serverList;
    private CuratorFramework client = null;

    private static final int MEMBER_COUNT = 10;

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

    private AtomicInteger resource = new AtomicInteger(0);

    @Test
    public void testReentrantMutex() throws InterruptedException {
        final String LOCK_PATH = "/curator/lock/reentrant_mutex";
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ExecutorService executorService = Executors.newCachedThreadPool();

        IntStream.range(0, MEMBER_COUNT).forEach(i -> {
            executorService.submit(() -> {
                InterProcessMutex lock = new InterProcessMutex(client, LOCK_PATH);
                try {
                    countDownLatch.await();
                    lock.acquire(100L, TimeUnit.MICROSECONDS);

                    log.info("thread {} got the lock", i);
                    resource.addAndGet(i);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (lock.isAcquiredInThisProcess()) {
                            lock.release();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        });

        countDownLatch.countDown();

        Thread.sleep(1000L);

        log.info("resource={}", resource.get());

        executorService.shutdown();
        executorService.awaitTermination(1L, TimeUnit.SECONDS);
    }


    @Test
    public void testRevocable2() throws InterruptedException {
        final String LOCK_PATH = "/curator/lock/reentrant_mutex_revocable";
        ExecutorService executorService = Executors.newCachedThreadPool();

        List<CuratorFramework> clientList = new ArrayList<>();

        executorService.execute(() -> {
            CuratorFramework client = CuratorFrameworkFactory
                .newClient(serverList, new RetryNTimes(3, 1000));
            client.start();
            clientList.add(client);

            InterProcessMutex lock = new InterProcessMutex(client, LOCK_PATH);

            Thread currentThread = Thread.currentThread();

            lock.makeRevocable(new RevocationListener<InterProcessMutex>() {
                @Override
                public void revocationRequested(InterProcessMutex forLock) {
                    /**
                     * <b>IMPORTANT</b> this event is handled in another thread. that's why forLock.release() throws exception!
                     */
                    log.info("#1 lock holder thread={}; current event thread={}",
                        currentThread.getName(), Thread.currentThread().getName());
                    Assert.assertNotEquals(currentThread, Thread.currentThread());

                    log.info("#1 thread {} get revocation request",
                        Thread.currentThread().getName());

                    try {
                        if (forLock.isAcquiredInThisProcess()) {
                            currentThread.interrupt();

                            log.info("#1 try to revoke the lock");
                            /**
                             *
                             * <pre>
                             *     this code will throw exception:
                             *
                             if (forLock.isAcquiredInThisProcess()) {
                             forLock.release();
                             }
                             *
                             * </pre>
                             *
                             * this does not work as well:
                             * <pre>
                             *      lock.release();
                             * </pre>
                             *
                             */
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            boolean interrupted = false;
            try {
                lock.acquire();

                lock.getParticipantNodes().forEach(i -> log.info("#1 participant node={}", i));

                log.info("#1 thread=[{}] got the lock", Thread.currentThread().getName());

                /** does not work!
                 *
                 * while (true){}
                 */

            } catch (InterruptedException e) {
                interrupted = true;
                e.printStackTrace();

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (lock.isAcquiredInThisProcess() || interrupted) {
                    try {
                        log.info("#1 thread=[{}] release the lock",
                            Thread.currentThread().getName());
                        lock.release();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        Thread.sleep(1000L);

        executorService.execute(() -> {
            CuratorFramework client = CuratorFrameworkFactory
                .newClient(serverList, new RetryNTimes(3, 1000));
            client.start();
            clientList.add(client);

            InterProcessMutex lock = new InterProcessMutex(client, LOCK_PATH);

            try {

                Assert.assertEquals(1, lock.getParticipantNodes().size());
                String[] array = lock.getParticipantNodes().toArray(new String[]{});

                Revoker.attemptRevoke(client, array[0]);
                lock.acquire();

                log.info("#2 thread=[{}] got the lock", Thread.currentThread().getName());
                Thread.sleep(1000L);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (lock.isAcquiredInThisProcess()) {
                    try {
                        log.info("#2 thread=[{}] release the lock",
                            Thread.currentThread().getName());
                        lock.release();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        Thread.sleep(10000L);

        clientList.forEach(i -> i.close());

    }

}
