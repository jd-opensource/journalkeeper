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
package io.journalkeeper.core.transaction;

/**
 * @author LiYue
 * Date: 2019/10/22
 */
public enum TransactionEntryType {

    // persistent types
    TRANSACTION_START(0),
    TRANSACTION_ENTRY(1),
    TRANSACTION_PRE_COMPLETE(2),
    TRANSACTION_COMPLETE(3);

    private int value;

    TransactionEntryType(int value) {
        this.value = value;
    }

    public static TransactionEntryType valueOf(final int value) {
        switch (value) {
            case 0:
                return TRANSACTION_START;
            case 1:
                return TRANSACTION_ENTRY;
            case 2:
                return TRANSACTION_PRE_COMPLETE;
            case 3:
                return TRANSACTION_COMPLETE;
            default:
                throw new IllegalArgumentException("Illegal TransactionEntryType value!");
        }
    }

    public int value() {
        return value;
    }
}
