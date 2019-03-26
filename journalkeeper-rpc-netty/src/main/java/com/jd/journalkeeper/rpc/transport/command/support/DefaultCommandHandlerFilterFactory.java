package com.jd.journalkeeper.rpc.transport.command.support;

import com.jd.journalkeeper.rpc.transport.command.handler.filter.CommandHandlerFilter;
import com.jd.journalkeeper.rpc.transport.command.handler.filter.CommandHandlerFilterFactory;
import com.jd.journalkeeper.utils.spi.ServiceSupport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 默认命令调用链工厂
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2018/8/16
 */
public class DefaultCommandHandlerFilterFactory implements CommandHandlerFilterFactory {

    private List<CommandHandlerFilter> commandHandlerFilters;

    public DefaultCommandHandlerFilterFactory() {
        this.commandHandlerFilters = initCommandHandlerFilters();
    }

    @Override
    public List<CommandHandlerFilter> getFilters() {
        return Collections.unmodifiableList(commandHandlerFilters);
    }

    protected List<CommandHandlerFilter> initCommandHandlerFilters() {
        List<CommandHandlerFilter> commandHandlerFilters = getCommandHandlerFilters();
        commandHandlerFilters.sort(new CommandHandlerFilterComparator());
        return commandHandlerFilters;
    }

    protected List<CommandHandlerFilter> getCommandHandlerFilters() {
        return new ArrayList<>(ServiceSupport.loadAll(CommandHandlerFilter.class));
    }
}