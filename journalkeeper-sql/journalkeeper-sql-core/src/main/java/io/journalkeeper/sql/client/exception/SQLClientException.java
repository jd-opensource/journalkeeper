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
package io.journalkeeper.sql.client.exception;

import io.journalkeeper.sql.exception.SQLException;

/**
 * SQLClientException
 * author: gaohaoxiang
 * date: 2019/8/1
 */
public class SQLClientException extends SQLException {

    public SQLClientException() {
    }

    public SQLClientException(String message) {
        super(message);
    }

    public SQLClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public SQLClientException(Throwable cause) {
        super(cause);
    }

    public SQLClientException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}