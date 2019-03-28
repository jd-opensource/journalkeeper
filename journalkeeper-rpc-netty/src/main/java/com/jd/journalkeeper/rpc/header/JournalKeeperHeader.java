package com.jd.journalkeeper.rpc.header;

import com.jd.journalkeeper.rpc.remoting.transport.command.Direction;
import com.jd.journalkeeper.rpc.remoting.transport.command.Header;

/**
 * @author liyue25
 * Date: 2019-03-28
 */
public class JournalKeeperHeader implements Header {

    public final static int MAGIC = 0x3f4e93d7;
    private boolean oneWay;
    private int status;
    private String error;
    private int requestId;
    private Direction direction;
    private int version;
    private int type;
    private long sendTime;

    public JournalKeeperHeader(){}

    public JournalKeeperHeader(int version, boolean oneWay, Direction direction, int requestId, int type, long sendTime, short status, String error) {
        this.version = version;
        this.oneWay = oneWay;
        this.direction = direction;
        this.requestId = requestId;
        this.type = type;
        this.sendTime = sendTime;
        this.status = status;
        this.error = error;
    }

    @Override
    public boolean isOneWay() {
        return oneWay;
    }

    @Override
    public void setOneWay(boolean oneWay) {
        this.oneWay = oneWay;
    }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public void setStatus(int status) {
        this.status = status;
    }

    @Override
    public String getError() {
        return error;
    }

    @Override
    public void setError(String error) {
        this.error = error;
    }

    @Override
    public int getRequestId() {
        return requestId;
    }

    @Override
    public void setRequestId(int requestId) {
        this.requestId = requestId;
    }

    @Override
    public Direction getDirection() {
        return direction;
    }

    @Override
    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public void setType(int type) {
        this.type = type;
    }

    public long getSendTime() {
        return sendTime;
    }

    public void setSendTime(long sendTime) {
        this.sendTime = sendTime;
    }
}
