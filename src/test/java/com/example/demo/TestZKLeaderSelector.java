package com.example.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.RetryNTimes;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;

/**
 * Created by Administrator on 2017/6/26 0026.
 */
@Slf4j
public class TestZKLeaderSelector extends DemoApplicationTests {

    @Value("${zoo.serverList}")
    private String serverList;

    private static final long MAIN_THREAD_SLEEP_MS = 2000L;
    // set it > 0 to sleep so that can observe changes in zk
    private static final long TAKE_OWNERSHIP_WAIT_MS = -1L;

    /**
     * LeaderSelector clients will create ephemeral nodes under mutex path with value of its id
     * <pre>
     * [zk: 192.168.212.26:2181(CONNECTED) 26] ls /curator/example/leader_selector
     * [_c_3214dca2-5adb-407a-9764-f8bb1457866e-lock-0000000029, _c_ea9ab1ac-9bb7-479a-a0ab-e992245e6258-lock-0000000023,
     * _c_e5a79691-f261-4b02-b161-2e7380dcd5f6-lock-0000000024, _c_f524a22a-a18a-4145-9821-65f2ab764f8e-lock-0000000027,
     * _c_6a38f55c-a923-432e-828f-0a5e96862b93-lock-0000000028, _c_f229fbb9-9569-4084-8c04-5c20c985a2da-lock-0000000026,
     * _c_265ba71d-0658-4c7a-8730-84942db3c8ad-lock-0000000025]
     *
     * </pre>
     *
     * <pre>
     * [zk: 192.168.212.26:2181(CONNECTED) 39] get /curator/example/leader_selector/_c_227eb6aa-f88a-4f55-8127-43ee1b228d9e-lock-0000000048
     * s9
     * cZxid = 0x733d7c
     * ctime = Mon Jun 26 13:55:24 CST 2017
     * mZxid = 0x733d7c
     * mtime = Mon Jun 26 13:55:24 CST 2017
     * pZxid = 0x733d7c
     * cversion = 0
     * dataVersion = 0
     * aclVersion = 0
     * ephemeralOwner = 0x15cb04655d205ee
     * dataLength = 2
     * numChildren = 0
     *
     *
     * </pre>
     */
    @Test
    public void testLeaderElection() throws InterruptedException {
        final int CLIENT_COUNT = 10;
        final String MUTEX_LEADER_PATH = "/curator/example/leader_selector";
        List<CuratorFramework> clientList = new ArrayList<>();
        List<LeaderSelector> leaderSelectorList = new ArrayList<>();

        IntStream.range(0, CLIENT_COUNT).forEach(i -> {
            CuratorFramework client = CuratorFrameworkFactory
                .newClient(serverList, new RetryNTimes(3, 1000));

            clientList.add(client);

            /**
             * {@link org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter}  handles {@link org.apache.curator.framework.state.ConnectionState.SUSPENDED} and {@link org.apache.curator.framework.state.ConnectionState.LOST}
             */
            LeaderSelector leaderSelector = new LeaderSelector(client, MUTEX_LEADER_PATH,
                new LeaderSelectorListenerAdapter() {
                    @Override
                    public void takeLeadership(CuratorFramework client) throws Exception {
                        log.info("client {} got leadership", i);

                        if (TAKE_OWNERSHIP_WAIT_MS > 0) {
                            Thread.sleep(TAKE_OWNERSHIP_WAIT_MS);
                        }
                    }
                });

            leaderSelectorList.add(leaderSelector);

            try {
                client.start();
                leaderSelector.setId("s" + i);
                leaderSelector.start();

            } finally {

            }
        });

        Thread.sleep(MAIN_THREAD_SLEEP_MS);

        // clean up resource
        leaderSelectorList.forEach(j -> {
            j.close();
        });
        clientList.forEach(k -> {
            k.close();
        });

    }

}
