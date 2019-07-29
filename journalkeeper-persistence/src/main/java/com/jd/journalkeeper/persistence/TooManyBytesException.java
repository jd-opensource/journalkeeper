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
package com.jd.journalkeeper.persistence;

import java.nio.file.Path;

/**
 * append的数据太大时抛出
 * @author liyue25
 * Date: 2019-04-04
 */
public class TooManyBytesException extends RuntimeException {
    public TooManyBytesException() {
        super();
    }

    public TooManyBytesException(int length, int limit, Path path) {
        super(String.format("Append bytes length %d exceed limit %d, path: %s.", length, limit, path.toString()));
    }
}
