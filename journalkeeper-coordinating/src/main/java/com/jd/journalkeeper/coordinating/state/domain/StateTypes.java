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
package com.jd.journalkeeper.coordinating.state.domain;

/**
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public enum StateTypes {

    SET(0),

    GET(1),

    REMOVE(2),

    EXIST(3),

    COMPARE_AND_SET(4),

    LIST(5),

    ;

    private int type;

    StateTypes(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public static StateTypes valueOf(int type) {
        switch (type) {
            case 0:
                return SET;
            case 1:
                return GET;
            case 2:
                return REMOVE;
            case 3:
                return EXIST;
            case 4:
                return COMPARE_AND_SET;
            case 5:
                return LIST;
            default:
                throw new UnsupportedOperationException(String.valueOf(type));
        }
    }
}