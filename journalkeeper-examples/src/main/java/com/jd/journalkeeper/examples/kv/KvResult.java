package com.jd.journalkeeper.examples.kv;

import java.util.List;

/**
 * @author liyue25
 * Date: 2019-04-03
 */
public class KvResult {
    /**
     * GET返回的值
     */
    private String value;
    /**
     * LIST_KEYS 返回的keys
     */
    private List<String> keys;

    public KvResult() {}

    public KvResult(String value, List<String> keys) {
        this.value = value;
        this.keys = keys;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    public String getValue() {
        return value;
    }

    public List<String> getKeys() {
        return keys;
    }
}
