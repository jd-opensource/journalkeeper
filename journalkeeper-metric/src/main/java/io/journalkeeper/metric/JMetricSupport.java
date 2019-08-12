package io.journalkeeper.metric;

import io.journalkeeper.utils.format.Format;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.journalkeeper.metric.JMetricReport.*;

/**
 * @author LiYue
 * Date: 2019-08-06
 */
public class JMetricSupport {
    public static String format(JMetricReport report, TimeUnit latencyTimeUnit) {
        double [] latencyNs = report.latency();
        double [] latencies = new double[latencyNs.length];
        long div = TimeUnit.NANOSECONDS.convert(1L, latencyTimeUnit);

        for (int i = 0; i < latencies.length; i++) {
            latencies[i] = latencyNs[i] / div;
        }

        return String.format("Metric %s, tps: %s/s, traffic: %s/s, " +
                "latency(%s): AVG %.2f, TP50 %.2f, TP90 %.2f, TP9 %.2f, " +
                        "TP99 %.2f, TP999 %.2f, TP9999 %.2f, MAX %.2f, " +
                "total requests: %s, total traffic: %s",
                report.name(), Format.formatWithComma(report.requestsPs()), Format.formatSize(report.trafficPs()),
                latencyTimeUnit.name(),
                latencies[TP_AVG], latencies[TP_50], latencies[TP_90], latencies[TP_95],
                latencies[TP_99], latencies[TP_999], latencies[TP_9999], latencies[TP_MAX],
                Format.formatWithComma(report.requestsTotal()), Format.formatSize(report.trafficTotal()));
    }
    public static String formatNs(JMetricReport report) {
        return  format(report, TimeUnit.NANOSECONDS);
    }
}
