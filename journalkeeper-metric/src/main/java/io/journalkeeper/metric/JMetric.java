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
package io.journalkeeper.metric;

/**
 * 用于监控流量，请求次数和时延的接口，使用方法：
 *
 * // 开始处理一批数据之前调用start()方法
 * jMetric.start();
 *
 * // 处理数据的业务逻辑在这儿
 * ....
 *
 * // 处理完成后调用end()方法记录监控信息, 1
 * // 024为本次处理的数据长度，用于统计流量
 *
 * jMetric.end(1024)
 *
 * @author LiYue
 * Date: 2019-08-06
 */
public interface JMetric {
    /**
     * 处理开始时调用
     */
    void start();

    /**
     * 处理结束是调用
     */
    default void end() {
        end(0L);
    }

    /**
     * 处理结束时调用，并记录流量。
     * @param traffic 本次处理的流量，单位Byte。
     */
    void end(long traffic);


    default void end(MetricCollector<Long> trafficCollector) {
        end(trafficCollector.collect());
    }
    /**
     * 记录一次处理。效果等同于一次start() 和 end()。
     * 同 mark(0L);
     */
    default void mark() {
        mark(0L);
    }

    /**
     * 记录一次处理。效果等同于一次start() 和 end()。
     * 同 mark(latencyNs, 0L);
     * @param latencyNs 本次处理时延，单位ns。
     */
    default void mark(long latencyNs) {
        mark(latencyNs, 0L);
    }


    default void mark(MetricCollector<Long> latencyCollector, MetricCollector<Long> trafficCollector) {
        mark(latencyCollector.collect(), trafficCollector.collect());
    }


    /**
     * 记录一次处理。效果等同于一次start() 和 end()。
     *
     * @param latencyNs 本次处理时延，单位ns。
     * @param traffic 本次处理流量， 单位Byte；
     */
    void mark(long latencyNs, long traffic);

    /**
     * 删除之前的所有的记录，重置所有计数器。
     */
    void reset();

    /**
     * 读取计数报告
     * @return 计数报告
     */
    JMetricReport get();

    /**
     * 读取计数报告并重置计数。
     * @return 计数报告
     */
    default JMetricReport getAndReset() {
        try {
            return get();
        } finally {
            reset();
        }
    }

    /**
     * 名字。
     * @return 名字。
     */
    String name();

}
