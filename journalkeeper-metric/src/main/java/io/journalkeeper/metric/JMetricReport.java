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
package io.journalkeeper.metric;

import java.util.Arrays;

/**
 * 计数报告快照。生成后数值就不再变化。
 * @author LiYue
 * Date: 2019-08-06
 */
public interface JMetricReport {
    int TP_AVG = 0;
    int TP_50 = 1;
    int TP_90 = 2;
    int TP_95 = 3;
    int TP_99 = 4;
    int TP_999 = 5;
    int TP_9999 = 6;
    int TP_MAX = 7;

    /**
     * 读取总流量。
     * @return 流量，单位Byte。
     */
    long trafficTotal();

    /**
     * 请求总次数。
     * @return 请求次数。
     */
    long requestsTotal();

    /**
     * 读取流量/每秒。
     * @return 流量，单位Bytes per Second。
     */
    long trafficPs();

    /**
     * 请求总次数/每秒。
     * @return 请求次数/每秒。
     */
    long requestsPs();

    /**
     * 读取时延。
     *
     * @return 返回固定长度为8的数组，每一个元素的含义如下：
     * [平均时延, TP50, TP90, TP95, TP99, TP999, TP9999, 最大时延]
     * 单位为ns。
     */
    double [] latency();

    /**
     * 生产报告的时间
     * @return 生产报告的时间。
     */
    long reportTime();

    /**
     * 名字。
     * @return 名字。
     */
    String name();
}
