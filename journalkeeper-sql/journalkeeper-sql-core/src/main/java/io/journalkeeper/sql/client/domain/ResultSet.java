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
package io.journalkeeper.sql.client.domain;

import java.util.List;
import java.util.Map;

/**
 * ResultSet
 * author: gaohaoxiang
 * date: 2019/8/15
 */
public class ResultSet {

    private List<Map<String, String>> rows;

    public ResultSet() {

    }

    public ResultSet(List<Map<String, String>> rows) {
        this.rows = rows;
    }

    public List<Map<String, String>> getRows() {
        return rows;
    }

    public void setRows(List<Map<String, String>> rows) {
        this.rows = rows;
    }

    @Override
    public String toString() {
        return "ResultSet{" +
                "rows=" + rows +
                '}';
    }
}