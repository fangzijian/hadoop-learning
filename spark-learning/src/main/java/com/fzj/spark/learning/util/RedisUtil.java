package com.fzj.spark.learning.util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.AsyncClusterPipeline;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class RedisUtil {

    private static volatile JedisCluster jedisCluster = null;

    public static Set<HostAndPort> getHostAndPorts(String address) {
        Set<HostAndPort> hostAndPorts = new HashSet<>();
        Arrays.stream(address.split(",")).forEach(hostAndPort -> {
            String host = hostAndPort.split(":")[0];
            int port = Integer.parseInt(hostAndPort.split(":")[1]);
            hostAndPorts.add(new HostAndPort(host, port));
        });
        return hostAndPorts;
    }

    public static AsyncClusterPipeline getPipeline(String address) {
        Set<HostAndPort> hostAndPorts = getHostAndPorts(address);
        return new AsyncClusterPipeline(hostAndPorts, new GenericObjectPoolConfig(), 60000, 60000, 60000);
    }

    public static JedisCluster getJedisCluster(String address) {
        Set<HostAndPort> hostAndPorts = getHostAndPorts(address);
        if (null == jedisCluster) {
            synchronized (RedisUtil.class) {
                if (null == jedisCluster) {
                    jedisCluster = new JedisCluster(hostAndPorts, 60000, 60000, 10, new JedisPoolConfig());
                }
            }
        }
        return jedisCluster;
    }

    public static void closeJedisCluster() {
        if (null != jedisCluster) {
            try {
                jedisCluster.close();
            } catch (IOException ignored) {
            }
        }
    }

}
