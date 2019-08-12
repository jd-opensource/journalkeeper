package io.journalkeeper.core.metric;

import io.journalkeeper.metric.JMetric;
import io.journalkeeper.metric.JMetricReport;

/**
 * @author LiYue
 * Date: 2019-08-12
 */
public class DummyMetric implements JMetric {

    @Override
    public void start() {

    }

    @Override
    public void end(long traffic) {

    }

    @Override
    public void mark(long latencyNs, long traffic) {

    }

    @Override
    public void reset() {

    }

    @Override
    public JMetricReport get() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String name() {
        return "DUMMY";
    }
}
