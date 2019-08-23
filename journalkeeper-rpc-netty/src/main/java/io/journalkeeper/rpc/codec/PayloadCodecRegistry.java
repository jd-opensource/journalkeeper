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
package io.journalkeeper.rpc.codec;

import io.journalkeeper.rpc.remoting.transport.codec.PayloadCodecFactory;

/**
 * @author LiYue
 * Date: 2019-04-01
 */
public class PayloadCodecRegistry {
    public static void register(PayloadCodecFactory payloadCodecFactory) {
        payloadCodecFactory.register(new UpdateClusterStateRequestCodec());
        payloadCodecFactory.register(new UpdateClusterStateResponseCodec());
        payloadCodecFactory.register(new LastAppliedRequestCodec());
        payloadCodecFactory.register(new LastAppliedResponseCodec());
        payloadCodecFactory.register(new QueryStateRequestCodec());
        payloadCodecFactory.register(new QueryStateResponseCodec());
        payloadCodecFactory.register(new GetServersRequestCodec());
        payloadCodecFactory.register(new GetServersResponseCodec());
        payloadCodecFactory.register(new AddPullWatchRequestCodec());
        payloadCodecFactory.register(new AddPullWatchResponseCodec());
        payloadCodecFactory.register(new RemovePullWatchRequestCodec());
        payloadCodecFactory.register(new RemovePullWatchResponseCodec());
        payloadCodecFactory.register(new PullEventsRequestCodec());
        payloadCodecFactory.register(new PullEventsResponseCodec());
        payloadCodecFactory.register(new UpdateVotersRequestCodec());
        payloadCodecFactory.register(new UpdateVotersResponseCodec());


        payloadCodecFactory.register(new AsyncAppendEntriesRequestCodec());
        payloadCodecFactory.register(new AsyncAppendEntriesResponseCodec());
        payloadCodecFactory.register(new RequestVoteRequestCodec());
        payloadCodecFactory.register(new RequestVoteResponseCodec());
        payloadCodecFactory.register(new GetServerEntriesRequestCodec());
        payloadCodecFactory.register(new GetServerEntriesResponseCodec());
        payloadCodecFactory.register(new GetServerStateRequestCodec());
        payloadCodecFactory.register(new GetServerStateResponseCodec());

    }
}
