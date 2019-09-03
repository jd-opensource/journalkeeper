package io.journalkeeper.rpc.client;

import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.StatusCode;

/**
 * @author LiYue
 * Date: 2019-09-03
 */
public class ConvertRollResponse extends BaseResponse {
    public ConvertRollResponse() {
    }

    public ConvertRollResponse(Throwable throwable) {
        super(throwable);
    }

    public ConvertRollResponse(StatusCode statusCode) {
        super(statusCode);
    }
}
