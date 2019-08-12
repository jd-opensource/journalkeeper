package io.journalkeeper.metric;

/**
 * @author LiYue
 * Date: 2019-08-06
 */
public interface JMetricFactory {
    JMetric create();
    JMetric create(String name);
}
