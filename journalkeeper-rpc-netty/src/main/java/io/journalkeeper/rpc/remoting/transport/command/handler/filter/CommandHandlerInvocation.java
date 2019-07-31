/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.rpc.remoting.transport.command.handler.filter;

import io.journalkeeper.rpc.remoting.transport.Transport;
import io.journalkeeper.rpc.remoting.transport.command.Command;
import io.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;
import io.journalkeeper.rpc.remoting.transport.exception.TransportException;

import java.util.Iterator;
import java.util.List;

/**
 * 命令处理器上下文
 * author: gaohaoxiang
 *
 * date: 2018/8/16
 */
public class CommandHandlerInvocation {

    private Transport transport;
    private Command command;
    private CommandHandler commandHandler;
    private Iterator<CommandHandlerFilter> filterIterator;
    private CommandHandlerContext context;

    public CommandHandlerInvocation(Transport transport, Command command, CommandHandler commandHandler, List<CommandHandlerFilter> filterList) {
        this.transport = transport;
        this.command = command;
        this.commandHandler = commandHandler;
        this.filterIterator = (filterList == null || filterList.isEmpty()) ? null : filterList.iterator();
    }

    public Command invoke() throws TransportException {
        if (filterIterator == null || !filterIterator.hasNext()) {
            return commandHandler.handle(transport, command);
        } else {
            return filterIterator.next().invoke(this);
        }
    }

    public Transport getTransport() {
        return transport;
    }

    public Command getCommand() {
        return command;
    }

    public CommandHandlerContext getContext() {
        if (context == null) {
            context = new CommandHandlerContext();
        }
        return context;
    }
}