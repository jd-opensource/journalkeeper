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
package io.journalkeeper.coordinating.state;

import io.journalkeeper.coordinating.state.domain.StateReadRequest;
import io.journalkeeper.coordinating.state.domain.StateResponse;
import io.journalkeeper.coordinating.state.domain.StateWriteRequest;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateFactory;

/**
 * CoordinatorStateFactory
 * author: gaohaoxiang
 *
 * date: 2019/5/30
 */
public class CoordinatorStateFactory implements StateFactory<StateWriteRequest, Void, StateReadRequest, StateResponse> {

    @Override
    public State<StateWriteRequest, Void, StateReadRequest, StateResponse> createState() {
        return new CoordinatingState(this);
    }
}