package com.jd.journalkeeper.core.api;

/**
 * @author liyue25
 * Date: 2019-03-20
 */
public interface StateFactory<E, Q, R> {
    State<E, Q, R> createState();
}
