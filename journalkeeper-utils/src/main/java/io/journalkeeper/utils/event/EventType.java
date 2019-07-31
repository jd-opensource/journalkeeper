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
package io.journalkeeper.utils.event;

/**
 * @author LiYue
 * Date: 2019-03-14
 */
public class EventType {
    // 0 - 999 Journal Store API(JK-JS API)
    public static final int ON_JOURNAL_CHANGE = 0;
    public static final int ON_STATE_CHANGE = 1;

    // 1000 - 1999 Journal Keeper Configuration API（JK-C API）
    public static final int ON_LEADER_CHANGE = 1000;
    public static final int ON_VOTERS_CHANGE = 1001;
}
