package com.jd.journalkeeper.rpc.remoting.example;

/**
 * @author liyue25
 * Date: 2019-03-28
 */
public class TestInterfaceImpl implements TestInterface {
    @Override
    public String hello(String name) {
        return "Hello! " + name;
    }
}
