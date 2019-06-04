package com.jd.journalkeeper.coordinating.keeper.store;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * KVStoreManager
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public class KVStoreManager {

    private static final Map<String, KVStoreFactory> factories;

    static {
        factories = loadFactories();
    }

    protected static Map<String, KVStoreFactory> loadFactories() {
        Map<String, KVStoreFactory> result = new HashMap<>();
        ServiceLoader<KVStoreFactory> factoryLoader = ServiceLoader.load(KVStoreFactory.class);
        Iterator<KVStoreFactory> iterator = factoryLoader.iterator();

        while (iterator.hasNext()) {
            KVStoreFactory factory = iterator.next();
            result.put(factory.type(), factory);
        }
        return result;
    }

    public static KVStoreFactory getFactory(String name) {
        if (StringUtils.isBlank(name)) {
            return factories.values().iterator().next();
        }
        return factories.get(name);
    }
}