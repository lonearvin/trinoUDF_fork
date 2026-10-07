/*
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
package io.trino.server;

import java.util.UUID;

/**
 * Identity of this JVM's server instance, regenerated on every start.
 * <p>
 * The node id is stable across restarts (it comes from the deployment), so it cannot tell two
 * incarnations of the same node apart. Dynamic plugin readiness is bound to this value, so a restarted
 * worker cannot inherit the readiness of the process it replaced.
 */
public final class NodeInstanceId
{
    private final String id = UUID.randomUUID().toString();

    public String getId()
    {
        return id;
    }
}
