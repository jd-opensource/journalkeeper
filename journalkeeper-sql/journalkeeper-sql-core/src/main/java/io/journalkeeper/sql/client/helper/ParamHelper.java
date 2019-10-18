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
package io.journalkeeper.sql.client.helper;

import java.util.ArrayList;
import java.util.List;

/**
 * ParamHelper
 * author: gaohaoxiang
 * date: 2019/8/7
 */
public class ParamHelper {

    public static List<Object> toList(Object... params) {
        if (params == null || params.length == 0) {
            return null;
        }
        List<Object> result = new ArrayList<>(params.length);
        for (int i = 0; i < params.length; i++) {
            result.add(params[i]);
        }
        return result;
    }

    public static Object[] toArray(List<Object> params) {
        if (params == null || params.isEmpty()) {
            return null;
        }
        Object[] result = new Object[params.size()];
        for (int i = 0; i < params.size(); i++) {
            result[i] = params.get(i);
        }
        return result;
    }
}