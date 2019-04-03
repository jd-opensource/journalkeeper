package com.jd.journalkeeper.examples.kv;

/**
 * kv 操作命令
 * @author liyue25
 * Date: 2019-04-03
 */
public class KvEntry {
    public final static int CMD_SET = 0;
    public final static int CMD_DEL = 2;

    private int cmd;
    private String key;
    private String value;

    public KvEntry(){}
    public KvEntry(int cmd, String key, String value) {
        this.cmd = cmd;
        this.key = key;
        this.value = value;
    }

    public int getCmd() {
        return cmd;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public void setCmd(int cmd) {
        this.cmd = cmd;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
