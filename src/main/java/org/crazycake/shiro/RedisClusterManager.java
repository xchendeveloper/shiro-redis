package org.crazycake.shiro;

import redis.clients.jedis.*;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * support jedis cluster
 *
 * @author chenxing
 * @create 2018-02-26 15:51
 **/

public class RedisClusterManager extends RedisBaseManager {

    // max attempts to connect to server
    private int maxAttempts = 3;

    private volatile JedisCluster jedisCluster = null;

    private void init() {
        synchronized (this) {
            if (jedisCluster == null) {
                jedisCluster = new JedisCluster(getHostAndPortSet(), timeout, soTimeout, maxAttempts, password, new JedisPoolConfig());
            }
        }
    }

    private Set<HostAndPort> getHostAndPortSet() {
        String[] hostAndPortArr = host.split(",");
        Set<HostAndPort> hostAndPorts = new HashSet<HostAndPort>();
        for (String hostAndPortStr : hostAndPortArr) {
            String[] hostAndPort = hostAndPortStr.split(":");
            hostAndPorts.add(new HostAndPort(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
        }
        return hostAndPorts;
    }


    protected JedisCluster getJedisCluster() {
        if (jedisCluster == null) {
            init();
        }
        return jedisCluster;
    }

    /**
     * get value from redis
     *
     * @param key
     * @return
     */
    public byte[] get(byte[] key) {
        if (key == null) {
            return null;
        }
        return getJedisCluster().get(key);
    }

    /**
     * set
     *
     * @param key
     * @param value
     * @return
     */
    public byte[] set(byte[] key, byte[] value) {
        if (key == null) {
            return null;
        }
        getJedisCluster().set(key, value);
        if (this.expire != 0) {
            getJedisCluster().expire(key, this.expire);
        }
        return value;
    }

    /**
     * set
     *
     * @param key
     * @param value
     * @param expire
     * @return
     */
    public byte[] set(byte[] key, byte[] value, int expire) {
        if (key == null) {
            return null;
        }
        getJedisCluster().set(key, value);
        if (this.expire != 0) {
            getJedisCluster().expire(key, expire);
        }
        return value;
    }

    /**
     * del
     *
     * @param key
     */
    public void del(byte[] key) {
        if (key == null) {
            return;
        }
        getJedisCluster().del(key);
    }

    /**
     * cluster size
     */
    public Long dbSize() {
        Long dbSize = 0L;
        Map<String, JedisPool> clusterNodes = getJedisCluster().getClusterNodes();
        for (String k : clusterNodes.keySet()) {
            JedisPool jp = clusterNodes.get(k);
            Jedis connection = jp.getResource();
            try {
                dbSize += connection.dbSize();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                connection.close();
            }
        }
        return dbSize;
    }

    /**
     * cluster keys
     *
     * @param pattern
     * @return
     */
    public Set<byte[]> keys(byte[] pattern) {
        Set<byte[]> keys = new HashSet<byte[]>();
        ScanParams params = new ScanParams();
        params.count(count);
        params.match(pattern);
        byte[] cursor = ScanParams.SCAN_POINTER_START_BINARY;
        ScanResult<byte[]> scanResult;
        do {
            scanResult = getJedisCluster().scan(cursor, params);
            keys.addAll(scanResult.getResult());
            cursor = scanResult.getCursorAsBytes();
        } while (scanResult.getStringCursor().compareTo(ScanParams.SCAN_POINTER_START) > 0);

        return keys;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

}
