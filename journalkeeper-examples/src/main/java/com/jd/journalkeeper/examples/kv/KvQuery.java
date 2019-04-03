package com.jd.journalkeeper.examples.kv;

/**
 * kv 操作命令
 * @author liyue25
 * Date: 2019-04-03
 */
public class KvQuery {
    public final static int CMD_GET = 1;
    public final static int CMD_LIST_KEYS = 3;

    private int cmd;
    private String key;

    public KvQuery() {}

    public KvQuery(int cmd, String key) {
        this.cmd = cmd;
        this.key = key;
    }

    public int getCmd() {
        return cmd;
    }

    public String getKey() {
        return key;
    }

    public void setCmd(int cmd) {
        this.cmd = cmd;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
