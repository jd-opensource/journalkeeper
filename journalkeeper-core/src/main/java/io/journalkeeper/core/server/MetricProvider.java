package io.journalkeeper.core.server;

import io.journalkeeper.metric.JMetric;

/**
 * @author LiYue
 * Date: 2019-09-10
 */
interface MetricProvider {
    /**
     * This method returns the metric instance of given name, instance will be created if not exists.
     * if config.isEnableMetric() equals false, just return a dummy metric.
     * @param name name of the metric
     * @return the metric instance of given name.
     */
    JMetric getMetric(String name);
    boolean isMetricEnabled();
    void removeMetric(String name);
}
