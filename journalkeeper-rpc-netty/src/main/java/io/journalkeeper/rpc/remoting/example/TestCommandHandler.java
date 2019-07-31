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
package io.journalkeeper.rpc.remoting.example;

import io.journalkeeper.rpc.remoting.transport.Transport;
import io.journalkeeper.rpc.remoting.transport.command.Command;
import io.journalkeeper.rpc.remoting.transport.command.Direction;
import io.journalkeeper.rpc.remoting.transport.command.Type;
import io.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;

/**
 * @author LiYue
 * Date: 2019-03-28
 */
public class TestCommandHandler implements CommandHandler, Type {
    private final TestInterface testImpl;

    public TestCommandHandler(TestInterface testImpl) {
        this.testImpl = testImpl;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        return new Command(
                new TestHeader(0, false, Direction.RESPONSE, command.getHeader().getRequestId(), -1, System.currentTimeMillis(), (short) 0, null),
                new StringPayload(testImpl.hello(((StringPayload )command.getPayload()).getPayload())));
    }

    @Override
    public int type() {
        return 1;
    }
}
