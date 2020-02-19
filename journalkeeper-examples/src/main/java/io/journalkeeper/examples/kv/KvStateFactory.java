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
package io.journalkeeper.examples.kv;

import io.journalkeeper.core.serialize.WrappedState;
import io.journalkeeper.core.serialize.WrappedStateFactory;

/**
 * @author LiYue
 * Date: 2019-04-03
 */
public class KvStateFactory implements WrappedStateFactory<String, String, String, String> {


    @Override
    public WrappedState<String, String, String, String> createState() {
        return new KvState();
    }
}
