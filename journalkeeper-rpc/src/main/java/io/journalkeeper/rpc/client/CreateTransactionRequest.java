package io.journalkeeper.rpc.client;

import java.util.Map;

/**
 * @author LiYue
 * Date: 2019/11/29
 */
public class CreateTransactionRequest {
    private final Map<String, String> context;

    public CreateTransactionRequest(Map<String, String> context) {
        this.context = context;
    }

    public Map<String, String> getContext() {
        return context;
    }
}
