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

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author LiYue
 * Date: 2019-03-15
 */
public abstract class BaseResponse {
    private StatusCode statusCode = StatusCode.SUCCESS;
    private String error = null;
    public BaseResponse () {}
    public BaseResponse(Throwable throwable) {
        if(null != throwable) {
            statusCode = StatusCode.EXCEPTION;
            setException(throwable);
        }
    }

    public BaseResponse(StatusCode statusCode) {
        setStatusCode(statusCode);
    }


    public void  setException(Throwable throwable) {
        StringWriter sw = new StringWriter();
        throwable.printStackTrace(new PrintWriter(sw));
        error = sw.toString();
    }

    public StatusCode getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(StatusCode statusCode) {
        this.statusCode = statusCode;
        this.error = statusCode.getMessage();
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public boolean success() {
        return statusCode == StatusCode.SUCCESS;
    }

    public String errorString() {
        return String.format("StatusCode: (%d)%s, ErrorMessage: %s", statusCode.getCode(), statusCode.getMessage(), error);
    }
}
