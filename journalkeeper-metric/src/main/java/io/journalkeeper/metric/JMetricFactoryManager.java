package io.journalkeeper.metric;

import io.journalkeeper.utils.spi.ServiceSupport;

/**
 * JMetricFactoryManager
 * author: gaohaoxiang
 * date: 2019/9/3
 */
public class JMetricFactoryManager {

    public static JMetricFactory getFactory() {
        return ServiceSupport.load(JMetricFactory.class);
    }
}