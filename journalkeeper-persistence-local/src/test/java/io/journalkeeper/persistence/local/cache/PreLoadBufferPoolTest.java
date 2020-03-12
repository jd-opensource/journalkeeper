package io.journalkeeper.persistence.local.cache;

import io.journalkeeper.utils.spi.ServiceSupport;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author LiYue
 * Date: 2020/3/12
 */
public class PreLoadBufferPoolTest {
    @Test
    public void testProperties() {
        System.setProperty("memory_cache.max_memory", "1g");
        System.setProperty("memory_cache.evict_ratio", String.valueOf(0.5));
        System.setProperty("memory_cache.core_ratio", String.valueOf(0.3));
        MemoryCacheManager memoryCacheManager = ServiceSupport.load(MemoryCacheManager.class);
        Assert.assertEquals(1024 * 1024 * 1024, memoryCacheManager.getMaxMemorySize());
    }
}
