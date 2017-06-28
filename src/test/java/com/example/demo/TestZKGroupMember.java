package com.example.demo;

import java.util.ArrayList;
import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.GroupMember;
import org.apache.curator.retry.RetryNTimes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;

/**
 * Created by Administrator on 2017/6/28 0028.
 */
public class TestZKGroupMember extends DemoApplicationTests {

    @Value("${zoo.serverList}")
    private String serverList;
    private CuratorFramework client = null;

    private static final int MEMBER_COUNT = 5;

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

    static class InitMode {

        public static final int CLEAN_EXISTING_PATH = 1;
        public static final int CREATE_PATH_IF_NOT_EXISTS = 2;
    }

    /**
     * <pre>
     *
     * java.lang.InterruptedException: null
     * at java.lang.Object.wait(Native Method) ~[na:1.8.0_112]
     * at java.lang.Object.wait(Object.java:502) ~[na:1.8.0_112]
     * at org.apache.zookeeper.ClientCnxn.submitRequest(ClientCnxn.java:1406)
     * ~[zookeeper-3.4.8.jar:3.4.8--1]
     * at org.apache.zookeeper.ZooKeeper.exists(ZooKeeper.java:1097) ~[zookeeper-3.4.8.jar:3.4.8--1]
     * at org.apache.zookeeper.ZooKeeper.exists(ZooKeeper.java:1130) ~[zookeeper-3.4.8.jar:3.4.8--1]
     * at org.apache.curator.utils.ZKPaths.mkdirs(ZKPaths.java:274) ~[curator-client-2.12.0.jar:na]
     * at org.apache.curator.framework.imps.CreateBuilderImpl$7.performBackgroundOperation(CreateBuilderImpl.java:561)
     * ~[curator-framework-2.12.0.jar:na]
     *
     * </pre>
     */
    @Test
    public void testGroupMemberWithPathNotExists() throws Exception {
        testGroupMemberHelper(InitMode.CLEAN_EXISTING_PATH);
    }


    @Test
    public void testGroupMemberWithExistingPath() throws Exception {
        testGroupMemberHelper(InitMode.CREATE_PATH_IF_NOT_EXISTS);
    }

    private void testGroupMemberHelper(int initMode) throws Exception {
        String membershipPath = "/curator/group";

        switch (initMode) {
            case InitMode.CLEAN_EXISTING_PATH:
                if (client.checkExists().forPath(membershipPath) != null) {
                    client.delete().deletingChildrenIfNeeded().forPath(membershipPath);
                }
                break;

            case InitMode.CREATE_PATH_IF_NOT_EXISTS:
                if (client.checkExists().forPath(membershipPath) == null) {
                    client.create().creatingParentsIfNeeded().forPath(membershipPath);
                }
                break;
            default:
                throw new RuntimeException("unexpected init mode");
        }

        List<GroupMember> groupMemberList = new ArrayList<>();
        for (int i = 0; i < MEMBER_COUNT; i++) {
            GroupMember group = new GroupMember(client, membershipPath, "m" + i,
                String.valueOf(i).getBytes());
            group.start();
            groupMemberList.add(group);
        }

        // can get the view of the group from one of the member
        Assert.assertEquals("0", new String(groupMemberList.get(0).getCurrentMembers().get("m0")));

        groupMemberList.forEach(i -> i.close());
    }
}
