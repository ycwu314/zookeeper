package com.example.demo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatch.State;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.RetryNTimes;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;

/**
 * Created by Administrator on 2017/6/26 0026.
 */
@Slf4j
public class TestZKLeaderLatch extends DemoApplicationTests {

    @Value("${zoo.serverList}")
    private String serverList;

    private static final int THREAD_COUNT = 10;

    @Test
    public void test() throws Exception {
        final String LATCH_PREFIX = "/curator/example/leader_latch";

        List<CuratorFramework> clientList = new ArrayList<>();
        List<LeaderLatch> leaderLatchList = new ArrayList<>();

        try {
            IntStream.range(0, THREAD_COUNT).forEach(i -> {
                CuratorFramework client = CuratorFrameworkFactory
                    .newClient(serverList, new RetryNTimes(3, 1000));
                clientList.add(client);

                LeaderLatch leaderLatch = new LeaderLatch(client, LATCH_PREFIX, "" + i);
                leaderLatch.addListener(new LeaderLatchListener() {
                    @Override
                    public void isLeader() {
                        log.info("leader latch {} is the leader", i);
                    }

                    @Override
                    public void notLeader() {

                    }
                });
                leaderLatchList.add(leaderLatch);

                try {
                    client.start();
                    leaderLatch.start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            Thread.sleep(2000L);

            LeaderLatch leader = null;
            for (LeaderLatch leaderLatch : leaderLatchList) {
                if (leaderLatch.hasLeadership()) {
                    leader = leaderLatch;
                    log.info("## leader is {}", leader.getId());
                    break;
                }
            }

            Assert.assertNotNull(leader);
            leader.close();

            // try to become leader, but no guarantee
            leaderLatchList.get(5).await(200, TimeUnit.MICROSECONDS);

            // <b>IMPORTANT<b> re-elect a leader need some time, if change too fast, will be no leader
            while (!leaderLatchList.get(0).getLeader().isLeader()) {
                log.info("no leader yet");
            }

            for (LeaderLatch leaderLatch : leaderLatchList) {
                if (leaderLatch.hasLeadership()) {
                    leader = leaderLatch;
                    log.info("## leader is {}", leader.getId());
                }
            }

        } finally {

            leaderLatchList.forEach(i -> {
                try {
                    if (!i.getState().equals(State.CLOSED)) {
                        i.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            clientList.forEach(i -> {
                i.close();
            });
        }


    }

}
