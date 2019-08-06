package io.journalkeeper.metrics.dropwizard;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import io.journalkeeper.metric.JMetric;
import io.journalkeeper.metric.JMetricReport;

/**
 * @author LiYue
 * Date: 2019-08-06
 */
public class MetricInstance implements JMetric {
    private final String name;
    private final MetricRegistry metrics;
    private Histogram latency;
    private Counter counter;
    private Counter traffic;
    private long startTimeNs;
    private long tmpStartTime = 0L;

    public MetricInstance(String name) {
        this.name = name;
        metrics = new MetricRegistry();
        reset();
    }

    private String latencyMetricName() {return name + "-latency";}
    private String counterMetricName() {return name + "-counter";}
    private String trafficMetricName() {return name + "-traffic";}

    @Override
    public void start() {
        tmpStartTime = System.nanoTime();
    }

    @Override
    public void end() {
        end(0L);
    }

    @Override
    public void end(long traffic) {
        mark(System.nanoTime() - tmpStartTime, traffic);
    }

    @Override
    public void mark(long latencyNs, long traffic) {
        this.counter.inc();
        this.latency.update(latencyNs);
        this.traffic.inc(traffic);
    }

    @Override
    public void reset() {
        metrics.remove(latencyMetricName());
        metrics.remove(counterMetricName());
        metrics.remove(trafficMetricName());
        this.latency = metrics.histogram(latencyMetricName());
        this.counter = metrics.counter(counterMetricName());
        this.traffic = metrics.counter(trafficMetricName());
        startTimeNs = System.nanoTime();
    }

    @Override
    public JMetricReport get() {

        return new MetricReport(name, counter.getCount(), traffic.getCount(),
                latency.getSnapshot(), startTimeNs, System.nanoTime());
    }


    @Override
    public String name() {
        return name;
    }
}
