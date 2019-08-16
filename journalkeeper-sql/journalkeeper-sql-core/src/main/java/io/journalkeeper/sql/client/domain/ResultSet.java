package io.journalkeeper.sql.client.domain;

import java.util.List;
import java.util.Map;

/**
 * ResultSet
 * author: gaohaoxiang
 * date: 2019/8/15
 */
public class ResultSet {

    private List<Map<String, String>> rows;

    public ResultSet() {

    }

    public ResultSet(List<Map<String, String>> rows) {
        this.rows = rows;
    }

    public List<Map<String, String>> getRows() {
        return rows;
    }

    public void setRows(List<Map<String, String>> rows) {
        this.rows = rows;
    }

    @Override
    public String toString() {
        return "ResultSet{" +
                "rows=" + rows +
                '}';
    }
}