package com.jd.journalkeeper.rpc;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author liyue25
 * Date: 2019-03-15
 */
public abstract class BaseResponse {
    private StatusCode statusCode = StatusCode.SUCCESS;
    private String error = null;
    public BaseResponse () {}
    public BaseResponse(Throwable throwable) {
        if(null != throwable) {
            statusCode = StatusCode.EXCEPTION;
            setException(throwable);
        }
    }

    public BaseResponse(StatusCode statusCode) {
        setStatusCode(statusCode);
    }


    public void  setException(Throwable throwable) {
        StringWriter sw = new StringWriter();
        throwable.printStackTrace(new PrintWriter(sw));
        error = sw.toString();
    }

    public StatusCode getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(StatusCode statusCode) {
        this.statusCode = statusCode;
        this.error = statusCode.getMessage();
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public boolean success() {
        return statusCode == StatusCode.SUCCESS;
    }

    public String errorString() {
        return String.format("StatusCode: (%d)%s, ErrorMessage: %s", statusCode.getCode(), statusCode.getMessage(), error);
    }
}
