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
package io.journalkeeper.core.state;

/**
 * @author LiYue
 * Date: 2019/11/20
 */

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Server当前集群配置的状态机，线程安全。包括二个状态：
 * 普通状态：常态。
 * 共同一致状态：变更进群配置过程中的中间状态。
 */
public class ConfigState {
    private final List<URI> configNew = new ArrayList<>(3);
    private final List<URI> configOld = new ArrayList<>(3);
    private boolean jointConsensus;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // all voters include configNew and configOld
    private List<URI> allVoters = new ArrayList<>(3);

    public ConfigState(List<URI> configOld, List<URI> configNew) {
        jointConsensus = true;
        this.configOld.addAll(configOld);
        this.configNew.addAll(configNew);

        buildAllVoters();
    }

    private void buildAllVoters() {
        allVoters = Stream.concat(configNew.stream(), configOld.stream())
                .distinct()
                .collect(Collectors.toList());
    }

    public ConfigState(List<URI> configNew) {
        jointConsensus = false;
        this.configNew.addAll(configNew);
        buildAllVoters();
    }

    public List<URI> getConfigNew() {
        rwLock.readLock().lock();
        try {
            return new ArrayList<>(configNew);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public List<URI> getConfigOld() {
        rwLock.readLock().lock();
        try {
            return new ArrayList<>(configOld);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public boolean isJointConsensus() {
        rwLock.readLock().lock();
        try {
            return jointConsensus;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public List<URI> voters() {
        rwLock.readLock().lock();
        try {
            return allVoters;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void toNewConfig(Callable appendEntryCallable) throws Exception {
        rwLock.writeLock().lock();
        try {
            if(!jointConsensus) {
                throw new IllegalStateException("Invalid joint consensus state! expected: jointConsensus == true, actual: false.");
            }
            appendEntryCallable.call();
            jointConsensus = false;
            configOld.clear();
            buildAllVoters();

        }finally {
            rwLock.writeLock().unlock();
        }
    }

    public void toJointConsensus(List<URI> configNew, Callable appendEntryCallable) throws Exception {
        rwLock.writeLock().lock();
        try {
            if(jointConsensus) {
                throw new IllegalStateException("Invalid joint consensus state! expected: jointConsensus == false, actual: true.");
            }

            appendEntryCallable.call();
            jointConsensus = true;
            this.configOld.addAll(this.configNew);
            this.configNew.clear();
            this.configNew.addAll(configNew);
            buildAllVoters();

        }finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public ConfigState clone() {
        rwLock.readLock().lock();
        try {
            if(jointConsensus) {
                return new ConfigState(new ArrayList<>(configOld), new ArrayList<>(configNew));
            } else {
                return new ConfigState(new ArrayList<>(configNew));
            }

        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public String toString() {
        rwLock.readLock().lock();
        try {
            String str = "Voters config: jointConsensus: " +
                    jointConsensus + ", ";
            if(jointConsensus) {
                str += "old config: [" +
                        configOld.stream().map(URI::toString).collect(Collectors.joining(", ")) + "], ";
                str += "new config: [" +
                        configNew.stream().map(URI::toString).collect(Collectors.joining(", ")) + "].";
            } else {
                str += "config: [" +
                        configNew.stream().map(URI::toString).collect(Collectors.joining(", ")) + "].";
            }
            return str;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void rollbackToOldConfig() {
        rwLock.writeLock().lock();
        try {
            if(!jointConsensus) {
                throw new IllegalStateException("Invalid joint consensus state! expected: jointConsensus == true, actual: false.");
            }
            jointConsensus = false;
            configNew.clear();
            configNew.addAll(configOld);
            configOld.clear();
            buildAllVoters();

        }finally {
            rwLock.writeLock().unlock();
        }
    }

    public void rollbackToJointConsensus(List<URI> configOld) {
        rwLock.writeLock().lock();
        try {
            if(jointConsensus) {
                throw new IllegalStateException("Invalid joint consensus state! expected: jointConsensus == false, actual: true.");
            }

            jointConsensus = true;
            this.configOld.addAll(configOld);
            buildAllVoters();

        }finally {
            rwLock.writeLock().unlock();
        }
    }
}

