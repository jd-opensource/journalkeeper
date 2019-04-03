package com.jd.journalkeeper.examples.kv;

import com.jd.journalkeeper.core.api.JournalKeeperClient;

import java.util.List;

/**
 * @author liyue25
 * Date: 2019-04-03
 */
public class KvClient {
    private final JournalKeeperClient<KvEntry, KvQuery, KvResult> client;

    public KvClient(JournalKeeperClient<KvEntry, KvQuery, KvResult> client) {
        this.client = client;
    }

    public void set(String key, String value) {
        try {
            client.update(new KvEntry(KvEntry.CMD_SET, key, value)).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String get(String key) {
        try {
            return client
                    .query(new KvQuery(KvQuery.CMD_GET, key))
                    .get()
                    .getValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void del(String key) {
        try {
            client.update(new KvEntry(KvEntry.CMD_DEL, key, null)).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    public List<String> listKeys() {
        try {
            return client
                    .query(new KvQuery(KvQuery.CMD_LIST_KEYS, null))
                    .get()
                    .getKeys();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
