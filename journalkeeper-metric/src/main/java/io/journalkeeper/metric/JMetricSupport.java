package io.journalkeeper.metric;

import io.journalkeeper.utils.format.Format;

import static io.journalkeeper.metric.JMetricReport.*;

/**
 * @author LiYue
 * Date: 2019-08-06
 */
public class JMetricSupport {

    public static String format(JMetricReport report) {
        double [] latencies = report.latencyMs();
        return String.format("Metric %s, tps: %s/s, traffic: %s/s, " +
                "latency(ms): AVG %.2f, TP50 %.2f, TP90 %.2f, TP9 %.2f, " +
                        "TP99 %.2f, TP999 %.2f, TP9999 %.2f, MAX %.2f, " +
                "total requests: %s, total traffic: %s",
                report.name(), Format.formatWithComma(report.requestsPs()), Format.formatSize(report.trafficPs()),
                latencies[TP_AVG], latencies[TP_50], latencies[TP_90], latencies[TP_95],
                latencies[TP_99], latencies[TP_999], latencies[TP_9999], latencies[TP_MAX],
                Format.formatWithComma(report.requestsTotal()), Format.formatSize(report.trafficTotal()));
    }
    public static String formatNs(JMetricReport report) {
        double [] latencies = report.latency();
        return String.format("Metric %s, tps: %s/s, traffic: %s/s, " +
                "latency(ns): AVG %.2f, TP50 %.2f, TP90 %.2f, TP9 %.2f, " +
                        "TP99 %.2f, TP999 %.2f, TP9999 %.2f, MAX %.2f, " +
                "total requests: %s, total traffic: %s",
                report.name(), Format.formatWithComma(report.requestsPs()), Format.formatSize(report.trafficPs()),
                latencies[TP_AVG], latencies[TP_50], latencies[TP_90], latencies[TP_95],
                latencies[TP_99], latencies[TP_999], latencies[TP_9999], latencies[TP_MAX],
                Format.formatWithComma(report.requestsTotal()), Format.formatSize(report.trafficTotal()));
    }
}
