package com.github.pires;

import junit.framework.Assert;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MyMapTest {

    private Ignite ignite;
    private IgniteCache<String, MyMap> mapCache;

    @BeforeClass
    private void setup() {
        // Ignite configuration
        final IgniteConfiguration igniteConfig = new IgniteConfiguration();

        // disable metrics
        igniteConfig.setMetricsLogFrequency(0);

        // go, go Ignite
        ignite = Ignition.start(igniteConfig);

        // configure device filters cache
        final CacheConfiguration<String, MyMap> mapCacheConfig = new CacheConfiguration<>();
        mapCacheConfig.setCacheMode(CacheMode.PARTITIONED);
        mapCacheConfig.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        mapCacheConfig.setAtomicWriteOrderMode(CacheAtomicWriteOrderMode.PRIMARY);
        mapCacheConfig.setBackups(2);
        mapCacheConfig.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
        mapCacheConfig.setStartSize(10 ^ 6); // 1 million
        mapCacheConfig.setOffHeapMaxMemory(0);
        mapCacheConfig.setSwapEnabled(false);
        mapCacheConfig.setName("cache");

        // register cache config
        ignite.addCacheConfiguration(mapCacheConfig);

        // get cache
        mapCache = ignite.getOrCreateCache(mapCacheConfig);
    }

    @Test
    public void test_put() {
        final MyMap map = new MyMap();
        map.put("a", "1");
        mapCache.put("x", map);
    }

    @Test(dependsOnMethods = "test_put")
    public void test_get() {
        final MyMap map = mapCache.get("x");
        Assert.assertNotNull(map.get("a"));

    }

}
