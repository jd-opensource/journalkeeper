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
package io.journalkeeper.rpc;

import java.util.HashMap;
import java.util.Map;

/**
 * Response 状态码
 * 范围：0 ~ 254
 * @author LiYue
 * Date: 2019-03-29
 */
public enum StatusCode {


    SUCCESS(0, "SUCCESS"),

    //001 ~ 100 Common
    UNKNOWN_ERROR(1, "UNKNOWN_ERROR"),
    EXCEPTION(2, "EXCEPTION"),
    TRANSPORT_FAILED(3, "TRANSPORT_FAILED"),
    SERVER_BUSY(4, "SERVER_BUSY"),
    NOT_LEADER(100, "NOT_LEADER"),

    // 101 ~ 200 Client Server Rpc
    PULL_WATCH_ID_NOT_EXISTS(101, "PULL_WATCH_ID_NOT_EXISTS"),


    // 201 ~ 254 Server Rpc
    INDEX_UNDERFLOW(201, "INDEX_UNDERFLOW"),
    INDEX_OVERFLOW(202, "INDEX_OVERFLOW");

    private static Map<Integer, StatusCode> codes = new HashMap<>();
    private int code;
    private String message;

    static {
        for (StatusCode jmqCode : StatusCode.values()) {
            codes.put(jmqCode.code, jmqCode);
        }
    }

    StatusCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    /**
     * 获取错误代码
     *
     * @param code
     * @return
     */
    public static StatusCode valueOf(int code) {
        return codes.get(code);
    }

    public int getCode() {
        return code;
    }

    public String getMessage(Object... args) {
        if (args.length < 1) {
            return message;
        }
        return String.format(message, args);
    }
}
