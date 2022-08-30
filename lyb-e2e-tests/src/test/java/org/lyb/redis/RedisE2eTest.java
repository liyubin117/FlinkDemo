package org.lyb.redis;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;

import static org.junit.Assert.assertEquals;

public class RedisE2eTest {
    private static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:3.0.6")).withExposedPorts(6379);

    @BeforeClass
    public static void startContainer() {
        redis.start();
    }

    @AfterClass
    public static void stopContainer() {
        redis.stop();
    }

    @Test
    public void testPutUseJedis() {
        Jedis jedis = new Jedis(redis.getHost(), redis.getFirstMappedPort());
        jedis.set("foo", "bar");
        assertEquals(jedis.get("foo"), "bar");
    }

    @Test
    public void testPutUseRedisClient() {
        RedisBackedCache cache = new RedisBackedCache(redis.getHost(), redis.getFirstMappedPort());
        cache.put("foo", "FOO");
        String foundObject = cache.get("foo");
        assertEquals(foundObject, "FOO");
    }
}
