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
package io.journalkeeper.core;

import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.StateFactory;

import java.io.Serializable;
import java.util.Properties;

/**
 * 使用JDK内置序列化方法的BootStrap
 * @author LiYue
 * Date: 2019-08-12
 */
public class JdkSerializerBootStrap<E extends Serializable, ER extends Serializable, Q extends Serializable, QR extends Serializable>
        extends BootStrap<E, ER, Q, QR> {
    public JdkSerializerBootStrap(RaftServer.Roll roll, StateFactory<E, ER, Q, QR> stateFactory,
                                  Class<E> eClass, Class<ER> erClass, Class<Q> qClass, Class<QR> qrClass,
                                  Properties properties) {
        super(roll, stateFactory,
                JdkSerializerFactory.createSerializer(eClass),
                JdkSerializerFactory.createSerializer(erClass),
                JdkSerializerFactory.createSerializer(qClass),
                JdkSerializerFactory.createSerializer(qrClass),
                properties);
    }
}
