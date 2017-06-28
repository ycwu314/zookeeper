package com.example.demo;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;

/**
 * Created by Administrator on 2017/6/28 0028.
 */
@Slf4j
public class TestZKPersistentEphemeralNode extends DemoApplicationTests {


    @Value("${zoo.serverList}")
    private String serverList;
    private static final String NAMESPACE = "curator2";
    private CuratorFramework client;

    @Before
    public void init() {
        client = CuratorFrameworkFactory.newClient(serverList, new RetryNTimes(3, 1000));
        client.start();
    }

    @After
    public void destroy() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void testPersistent() {
        PersistentNode persistentNode = null;
        final String DATA = "3333";
        try {
            persistentNode = new PersistentNode(client.usingNamespace(NAMESPACE),
                CreateMode.PERSISTENT, false, "/persist", "".getBytes());
            persistentNode.start();
            persistentNode.waitForInitialCreate(2, TimeUnit.SECONDS);
            persistentNode.setData(DATA.getBytes());

            Assert.assertArrayEquals(DATA.getBytes(), persistentNode.getData());

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                /**
                 * if not close the PersistentNode instance, and directly exits, will have exceptions:
                 * <pre>
                 java.lang.IllegalStateException: instance must be started before calling this method
                 at org.apache.curator.shaded.com.google.common.base.Preconditions.checkState(Preconditions.java:176) ~[curator-client-2.12.0.jar:na]
                 at org.apache.curator.framework.imps.CuratorFrameworkImpl.checkExists(CuratorFrameworkImpl.java:367) ~[curator-framework-2.12.0.jar:na]
                 at org.apache.curator.framework.recipes.nodes.PersistentNode.watchNode(PersistentNode.java:421) ~[curator-recipes-2.12.0.jar:na]
                 at org.apache.curator.framework.recipes.nodes.PersistentNode.access$100(PersistentNode.java:58) ~[curator-recipes-2.12.0.jar:na]

                 * </pre>
                 */
                if (persistentNode != null) {
                    persistentNode.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testPersistentSequential() throws Exception {
        final String NODE_PATH = "/persist_seq_";
        PersistentNode node = new PersistentNode(client.usingNamespace(NAMESPACE),
            CreateMode.PERSISTENT_SEQUENTIAL, false, NODE_PATH, "".getBytes());
        node.start();
        node.waitForInitialCreate(1, TimeUnit.SECONDS);

        // getActualPath() does not contains NAMESPACE part!!!
        String actualPath = node.getActualPath();
        log.info("getActualPath={}", actualPath);
        Assert.assertEquals(-1, actualPath.indexOf(NAMESPACE));

        // CreateMode.PERSISTENT_SEQUENTIAL : if close then delete the node !!! it is not persistent at all
        node.close();
        Assert.assertNull(client.checkExists().forPath(actualPath));
    }


    @Test
    public void testEphemeral() throws Exception {
        final byte[] DATA = "1111".getBytes();
        PersistentNode node = new PersistentNode(client.usingNamespace(NAMESPACE),
            CreateMode.EPHEMERAL, false, "/ephemeral", "".getBytes());
        node.start();
        node.waitForInitialCreate(1, TimeUnit.SECONDS);
        node.setData(DATA);
        log.info("{}", node.getActualPath());
        Assert.assertArrayEquals(DATA, node.getData());
        node.close();
    }

    @Test
    public void testEphemeralSequential() throws Exception {
        final byte[] DATA = "1111".getBytes();
        PersistentNode node = new PersistentNode(client.usingNamespace(NAMESPACE),
            CreateMode.PERSISTENT_SEQUENTIAL, false, "/ephemeral_seq", "".getBytes());
        node.start();
        node.waitForInitialCreate(1, TimeUnit.SECONDS);
        node.setData(DATA);
        log.info("{}", node.getActualPath());
        Assert.assertArrayEquals(DATA, node.getData());
        node.close();
    }

}
