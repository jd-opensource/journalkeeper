package com.jd.journalkeeper.persistence.local.journal;

/**
 * 文件已损坏
 * @author liyue25
 * Date: 2018/9/29
 */
public class CorruptedStoreException extends RuntimeException {
    public CorruptedStoreException() {super();}
    public CorruptedStoreException(String msg) {super(msg);}
}
