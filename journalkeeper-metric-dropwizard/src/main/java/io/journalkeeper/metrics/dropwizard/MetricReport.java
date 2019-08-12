package io.journalkeeper.metrics.dropwizard;

import com.codahale.metrics.Snapshot;
import io.journalkeeper.metric.JMetricReport;

/**
 * @author LiYue
 * Date: 2019-08-06
 */
public class MetricReport implements JMetricReport {

    private final long counter;
    private final long traffic;
    private final Snapshot latency;
    private final long start, end;
    private final String name;

    public MetricReport(String name, long counter, long traffic, Snapshot latency, long start, long end) {
        this.counter = counter;
        this.traffic = traffic;
        this.latency = latency;
        this.start = start;
        this.end = end;
        this.name = name;
    }

    @Override
    public long trafficTotal() {
        return traffic;
    }

    @Override
    public long requestsTotal() {
        return counter;
    }

    @Override
    public long trafficPs() {
        return traffic / ((end - start) / 1000000000L);
    }

    @Override
    public long requestsPs() {
        return counter / ((end - start) / 1000000000L);
    }

    @Override
    public double [] latency() {
        return new double[] {
                latency.getMean(),
                latency.getValue(0.5),
                latency.getValue(0.90),
                latency.getValue(0.95),
                latency.getValue(0.99),
                latency.getValue(0.999),
                latency.getValue(0.9999),
                latency.getMax()
        };
    }

    @Override
    public long reportTime() {
        return end;
    }

    @Override
    public String name() {
        return name;
    }
}
