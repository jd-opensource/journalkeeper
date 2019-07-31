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
import io.journalkeeper.rpc.remoting.transport.command.Header;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author LiYue
 * Date: 2019-03-28
 */
public class TestInterfaceStub implements TestInterface {

    private AtomicInteger requestId = new AtomicInteger(0);
    private final Transport transport;

    public TestInterfaceStub(Transport transport) {
        this.transport = transport;
    }

    @Override
    public String hello(String name) {
        Header header = new TestHeader();
        header.setOneWay(false);
        header.setDirection(Direction.REQUEST);
        header.setVersion(0);
        header.setType(1);
        header.setRequestId(requestId.getAndIncrement());

        Command command = new Command(header,new StringPayload(name));
        Command resp = transport.sync(command);
        return ((StringPayload) resp.getPayload()).getPayload();

    }
}
