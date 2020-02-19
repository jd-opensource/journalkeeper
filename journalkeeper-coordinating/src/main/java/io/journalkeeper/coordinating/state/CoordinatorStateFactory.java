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
package io.journalkeeper.coordinating.state;

import io.journalkeeper.coordinating.state.domain.ReadRequest;
import io.journalkeeper.coordinating.state.domain.ReadResponse;
import io.journalkeeper.coordinating.state.domain.WriteRequest;
import io.journalkeeper.coordinating.state.domain.WriteResponse;
import io.journalkeeper.core.serialize.WrappedState;
import io.journalkeeper.core.serialize.WrappedStateFactory;

/**
 * CoordinatorStateFactory
 * author: gaohaoxiang
 *
 * date: 2019/5/30
 */
public class CoordinatorStateFactory implements WrappedStateFactory<WriteRequest, WriteResponse, ReadRequest, ReadResponse> {

    @Override
    public WrappedState<WriteRequest, WriteResponse, ReadRequest, ReadResponse> createState() {
        return new CoordinatingState();
    }
}