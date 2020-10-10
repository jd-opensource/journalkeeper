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

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static io.journalkeeper.core.entry.internal.InternalEntryType.TYPE_UPDATE_VOTERS_S2;

/**
 * @author LiYue
 * Date: 2019-08-26
 */
public class UpdateVotersS2Entry extends InternalEntry implements Serializable {
    private static final long serialVersionUID = 1L;
    private final long epoch;

    private final List<URI> configNew;
    private final List<URI> configOld;

    public UpdateVotersS2Entry(List<URI> configOld, List<URI> configNew, long epoch) {
        super(TYPE_UPDATE_VOTERS_S2);
        this.configNew = new ArrayList<>(configNew);
        this.configOld = new ArrayList<>(configOld);
        this.epoch = epoch;
    }

    public UpdateVotersS2Entry(List<URI> configOld, List<URI> configNew, long epoch, int version) {
        super(TYPE_UPDATE_VOTERS_S2, version);
        this.configNew = new ArrayList<>(configNew);
        this.configOld = new ArrayList<>(configOld);
        this.epoch = epoch;
    }

    public List<URI> getConfigNew() {
        return configNew;
    }

    public List<URI> getConfigOld() {
        return configOld;
    }

    public long getEpoch() {
        return epoch;
    }
}
