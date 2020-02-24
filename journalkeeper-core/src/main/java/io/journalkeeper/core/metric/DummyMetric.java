/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.core.metric;

import io.journalkeeper.metric.JMetric;
import io.journalkeeper.metric.JMetricReport;
import io.journalkeeper.metric.MetricCollector;

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
    public void mark(MetricCollector<Long> latencyCollector, MetricCollector<Long> trafficCollector) {

    }

    @Override
    public void end(MetricCollector<Long> trafficCollector) {

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
