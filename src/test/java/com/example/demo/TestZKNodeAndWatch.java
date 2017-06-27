package com.example.demo;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;

/**
 * Created by Administrator on 2017/6/22 0022.
 */
@Slf4j
public class TestZKNodeAndWatch extends DemoApplicationTests {

    @Value("${zoo.serverList}")
    private String serverList;

    private CuratorFramework client = null;
    private static final String PATH_PREFIX = "/curator/example/";


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
    public void testBasicCRUD() throws Exception {

        String path = PATH_PREFIX + "crud";
        String data = "hello world";

        String ret = client.create().creatingParentsIfNeeded().forPath(path, data.getBytes());
        Assert.assertEquals(path, ret);

        Assert.assertArrayEquals(data.getBytes(), client.getData().forPath(path));

        String newData = "not good";
        client.setData().forPath(path, newData.getBytes());
        Assert.assertArrayEquals(newData.getBytes(), client.getData().forPath(path));

        client.delete().deletingChildrenIfNeeded().forPath(path);
        // checkExists() : if not null the return a Stat object, otherwise return null
        Assert.assertNull(client.checkExists().forPath(path));

    }

    /**
     * watcher will only trigger once
     */
    @Test
    public void testWatch() throws Exception {
        String path = PATH_PREFIX + "watch";

        if (client.checkExists().forPath(path) == null) {
            client.create().forPath(path, null);
            log.info("created a path={}", path);
        }

        AtomicInteger watchEventCounter = new AtomicInteger(0);

        client.getData().usingWatcher(new CuratorWatcher() {
            @Override
            public void process(WatchedEvent event) throws Exception {
                if (event.getType().equals(EventType.NodeDataChanged)) {
                    log.info("** get node data change event..");
                    watchEventCounter.incrementAndGet();
                }
            }
        }).forPath(path);

        IntStream.range(0, 5).forEach(i -> {
            try {
                log.info("change node data to {}", i);
                client.setData().forPath(path, String.valueOf(i).getBytes());
                Thread.sleep(500L);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        client.delete().forPath(path);

        Assert.assertEquals(1, watchEventCounter.get());
    }

    ////////////////////////////////////////////////////////////////////

    /**
     * NodeCache can automatically re-watch.
     * but if change too fast, will lost node change event
     */
    @Test
    public void testNodeCacheGetSlowChangeWatchEvents() throws Exception {

        int nodeChangeTimes = 5;
        AtomicInteger nodeChangeEventCounter = new AtomicInteger(0);
        testNodeCacheHelper(true, nodeChangeTimes, nodeChangeEventCounter);

        Assert.assertEquals(nodeChangeTimes, nodeChangeEventCounter.get());
    }

    /**
     * NodeCache can automatically re-watch.
     * but if change too fast, will lost node change event
     */
    @Test
    public void testNodeCacheMissFastChangeWatchEvents() throws Exception {

        int nodeChangeTimes = 10;
        AtomicInteger nodeChangeEventCounter = new AtomicInteger(0);
        testNodeCacheHelper(false, nodeChangeTimes, nodeChangeEventCounter);

        Assert.assertNotEquals(nodeChangeTimes, nodeChangeEventCounter.get());
    }

    private void testNodeCacheHelper(boolean sleep, int nodeChangeTimes,
        AtomicInteger nodeChangeEventCounter)
        throws Exception {
        String path = PATH_PREFIX + "watch2";

        if (client.checkExists().forPath(path) == null) {
            client.create().forPath(path, null);
            log.info("created a path={}", path);
        }

        NodeCache nodeCache = new NodeCache(client, path);
        nodeCache.getListenable().addListener(() -> {
            log.info("got node change..");
            nodeChangeEventCounter.incrementAndGet();
        });

        try {
            nodeCache.start(true);

            IntStream.range(0, nodeChangeTimes).forEach(i -> {
                try {
                    client.setData().forPath(path, String.valueOf(i).getBytes());
                    log.info("current node data={}",
                        new String(nodeCache.getCurrentData().getData()));

                    if (sleep) {
                        Thread.sleep(1000L);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        } finally {
            nodeCache.close();
        }
    }

    /**
     * like NodeCache, re-watch events
     */
    @Test
    public void testPathChildrenCache() throws Exception {
        String path = PATH_PREFIX + "watch3";

        if (client.checkExists().forPath(path) == null) {
            client.create().forPath(path, null);
            log.info("created a path={}", path);
        }

        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, path, false);
        pathChildrenCache.getListenable().addListener((client, event) -> {
            log.info("change event[type={}, path={}]", event.getType().toString(),
                event.getData().getPath());
        });
        pathChildrenCache.rebuild();

        try {
            pathChildrenCache.start();

            String child = path + "/" + UUID.randomUUID().toString();
            client.create().creatingParentsIfNeeded()
                .forPath(child, "aaa".getBytes());
            Thread.sleep(100L);

            client.setData().forPath(child, "bbb".getBytes());
            Thread.sleep(100L);

            client.delete().forPath(child);
            Thread.sleep(100L);

            client.delete().deletingChildrenIfNeeded().forPath(path);
        } finally {
            pathChildrenCache.close();
        }
    }

    ////////////////////////////////////////////////////////////////////

}



