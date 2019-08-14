/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
        long div = (end - start) / 1000000000L;
        return div > 0 ? traffic / div: 0L;    }

    @Override
    public long requestsPs() {
        long div = (end - start) / 1000000000L;
        return div > 0 ? counter / div: 0L;
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
