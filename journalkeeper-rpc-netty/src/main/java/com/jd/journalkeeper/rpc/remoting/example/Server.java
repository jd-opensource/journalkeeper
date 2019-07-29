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
package com.jd.journalkeeper.rpc.remoting.example;

import com.jd.journalkeeper.rpc.remoting.transport.TransportServer;
import com.jd.journalkeeper.rpc.remoting.transport.command.support.DefaultCommandHandlerFactory;
import com.jd.journalkeeper.rpc.remoting.transport.config.ServerConfig;
import com.jd.journalkeeper.rpc.remoting.transport.support.DefaultTransportServerFactory;

/**
 * @author liyue25
 * Date: 2019-03-28
 */
public class Server {
    public static void main(String [] args) throws Exception {
        DefaultCommandHandlerFactory handlerFactory = new DefaultCommandHandlerFactory();
        handlerFactory.register(new TestCommandHandler(new TestInterfaceImpl()));
        DefaultTransportServerFactory defaultTransportServerFactory = new DefaultTransportServerFactory(
                new TestCodec(), handlerFactory
               );
        TransportServer server = defaultTransportServerFactory.bind(new ServerConfig(), "localhost", 9999);
        server.start();
        System.in.read();
        System.out.println("Stopping...");
        server.stop();


    }
}
