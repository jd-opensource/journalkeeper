package com.jd.journalkeeper.utils.threads;

/**
 * @author liyue25
 * Date: 2019-06-21
 */
public class ThreadBuilder {
    private String name;
    private long minSleep = -1L,maxSleep = -1L;
    private Boolean daemon;
    private Worker worker;
    private ExceptionHandler exceptionHandler;
    private ExceptionListener exceptionListener;
    private Condition condition;
    
    public static ThreadBuilder builder() {
        return new ThreadBuilder();
    }
    public ThreadBuilder doWork(Worker worker){
        this.worker = worker;
        return this;
    }

    public ThreadBuilder handleException(ExceptionHandler exceptionHandler){
        this.exceptionHandler = exceptionHandler;
        return this;
    }

    public ThreadBuilder onException(ExceptionListener exceptionListener){
        this.exceptionListener = exceptionListener;
        return this;
    }

    public ThreadBuilder name(String name){
        this.name = name;
        return this;
    }

    public ThreadBuilder sleepTime(long minSleep, long maxSleep){
        this.minSleep = minSleep;
        this.maxSleep = maxSleep;
        return this;
    }

    public ThreadBuilder daemon(boolean daemon) {
        this.daemon = daemon;
        return this;
    }

    public ThreadBuilder condition(Condition condition){
        this.condition = condition;
        return this;
    }

    public AsyncLoopThread build(){
        LoopThread loopThread = new LoopThread() {
            @Override
            void doWork() throws Throwable{
                worker.doWork();
            }

            @Override
            protected boolean handleException(Throwable t) {
                if(null != exceptionListener) exceptionListener.onException(t);
                if(null != exceptionHandler) {
                    return exceptionHandler.handleException(t);
                }else {
                    return super.handleException(t);
                }
            }

            @Override
            protected boolean condition() {
                if(null != condition) {
                    return condition.condition();
                }else {
                    return super.condition();
                }
            }
        };
        if(null != name) loopThread.setName(name);
        if(null != daemon) loopThread.setDaemon(daemon);
        if(this.minSleep >= 0) loopThread.minSleep = this.minSleep;
        if(this.maxSleep >= 0) loopThread.maxSleep = this.maxSleep;
        return loopThread;
    }
}
