package io.journalkeeper.metric;

/**
 * @author LiYue
 * Date: 2020/2/24
 */
public interface MetricCollector<T> {
    T collect();
}
