package io.journalkeeper.metrics.dropwizard;

import io.journalkeeper.metric.JMetric;
import io.journalkeeper.metric.JMetricFactory;

/**
 * @author LiYue
 * Date: 2019-08-06
 */
public class MetricFactory implements JMetricFactory {
    @Override
    public JMetric create() {
        return create("NO_NAME");
    }

    @Override
    public JMetric create(String name) {
        return new MetricInstance(name);
    }
}
