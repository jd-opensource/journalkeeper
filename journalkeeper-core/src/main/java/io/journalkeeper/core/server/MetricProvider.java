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
package io.journalkeeper.core.server;

import io.journalkeeper.metric.JMetric;

/**
 * @author LiYue
 * Date: 2019-09-10
 */
public interface MetricProvider {
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
