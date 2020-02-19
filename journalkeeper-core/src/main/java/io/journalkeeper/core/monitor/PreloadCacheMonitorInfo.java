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
package io.journalkeeper.core.monitor;

import io.journalkeeper.utils.format.Format;

/**
 * @author LiYue
 * Date: 2019/12/9
 */
public class PreloadCacheMonitorInfo {
    private long size;
    private long coreCount;
    private long maxCount;
    private long cachedCount;
    private long usedCount;

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getCoreCount() {
        return coreCount;
    }

    public void setCoreCount(long coreCount) {
        this.coreCount = coreCount;
    }

    public long getMaxCount() {
        return maxCount;
    }

    public void setMaxCount(long maxCount) {
        this.maxCount = maxCount;
    }

    public long getCachedCount() {
        return cachedCount;
    }

    public void setCachedCount(long cachedCount) {
        this.cachedCount = cachedCount;
    }

    public long getUsedCount() {
        return usedCount;
    }

    public void setUsedCount(long usedCount) {
        this.usedCount = usedCount;
    }

    @Override
    public String toString() {
        return "PreloadCacheMonitorInfo{" +
                "size=" + Format.formatSize(size) +
                ", coreCount=" + coreCount +
                ", maxCount=" + maxCount +
                ", cachedCount=" + cachedCount + "/" + Format.formatSize(size * cachedCount) +
                ", usedCount=" + usedCount + "/" + Format.formatSize(size * usedCount) +
                '}';
    }
}
