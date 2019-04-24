package com.jd.journalkeeper.utils.event;

/**
 * 事件拦截器
 * @author liyue25
 * Date: 2019-04-24
 */
public interface EventInterceptor {
    /**
     * 拦截事件
     * @param event 事件
     * @param eventBus 事件总线
     * @return true: 继续发送事件， false： 取消事件
     */
    boolean onEvent(Event event, EventBus eventBus);
}
