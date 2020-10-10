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
package io.journalkeeper.core.entry.internal;

/**
 * @author LiYue
 * Date: 2019-05-09
 */
public abstract class InternalEntry {
    final static int VERSION_LEGACY = -1;
    final static int BASE_VERSION = 1;
    private final InternalEntryType type;

    private final int version;
    protected InternalEntry(InternalEntryType type) {
        this(type, BASE_VERSION);
    }
    protected InternalEntry(InternalEntryType type, int version) {
        this.type = type;
        this.version = version;
    }

    public InternalEntryType getType() {
        return type;
    }

    public int getVersion() {
        return version;
    }
}
