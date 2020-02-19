/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.rpc.remoting.transport;

import io.journalkeeper.rpc.remoting.concurrent.EventListener;
import io.journalkeeper.rpc.remoting.event.TransportEvent;
import io.journalkeeper.rpc.remoting.service.LifeCycle;
import io.journalkeeper.rpc.remoting.transport.exception.TransportException;

import java.net.SocketAddress;

/**
 * 通信服务
 * author: gaohaoxiang
 *
 * date: 2018/8/13
 */
public interface TransportClient extends LifeCycle {

    /**
     * 创建连接，阻塞直到成功或失败
     *
     * @param address 地址
     * @return 通道
     * @throws TransportException 传输异常
     */
    Transport createTransport(final String address) throws TransportException;

    /**
     * 创建连接，阻塞直到成功或失败
     *
     * @param address           地址
     * @param connectionTimeout 连接超时
     * @return 通道
     * @throws TransportException 传输异常
     */
    Transport createTransport(final String address, final long connectionTimeout) throws TransportException;

    /**
     * 创建连接，阻塞直到成功或失败
     *
     * @param address 地址
     * @return 通道
     * @throws TransportException 传输异常
     */
    Transport createTransport(final SocketAddress address) throws TransportException;

    /**
     * 创建连接
     *
     * @param address           地址
     * @param connectionTimeout 连接超时
     * @return 通道
     * @throws TransportException 传输异常
     */
    Transport createTransport(final SocketAddress address, final long connectionTimeout) throws TransportException;

    /**
     * 添加监听器
     *
     * @param listener 监听器
     */
    void addListener(EventListener<TransportEvent> listener);

    /**
     * 移除监听器
     *
     * @param listener 监听器
     */
    void removeListener(EventListener<TransportEvent> listener);
}