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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Keeps active worker nodes out of task placement until the coordinator has verified that the worker's
 * running process holds the coordinator's dynamic plugin set.
 * <p>
 * Readiness is keyed by node id <em>and</em> process instance id. A worker that restarts keeps its node id
 * but gets a new instance id, so it cannot inherit the readiness recorded for the process it replaced.
 * <p>
 * Readiness is not required until the dynamic plugin feature is enabled, so a cluster that does not use the
 * feature places work on every active worker exactly as before, with no dependence on startup ordering.
 */
public final class DynamicPluginNodeReadiness
{
    private final Map<String, String> readyWorkers = new ConcurrentHashMap<>();
    private volatile boolean readinessRequired;

    public boolean isWorkerReady(String nodeIdentifier, String instanceId)
    {
        requireNonNull(nodeIdentifier, "nodeIdentifier is null");
        requireNonNull(instanceId, "instanceId is null");
        if (!readinessRequired) {
            return true;
        }
        return instanceId.equals(readyWorkers.get(nodeIdentifier));
    }

    public void requireReadiness()
    {
        readinessRequired = true;
    }

    public boolean isReadinessRequired()
    {
        return readinessRequired;
    }

    /**
     * Records that the given worker process holds the coordinator's plugin set. The instance id must be the
     * one the coordinator observed for the process that actually served the plugin state.
     */
    public void markReady(String nodeIdentifier, String instanceId)
    {
        readyWorkers.put(
                requireNonNull(nodeIdentifier, "nodeIdentifier is null"),
                requireNonNull(instanceId, "instanceId is null"));
    }

    public void markNotReady(String nodeIdentifier)
    {
        readyWorkers.remove(requireNonNull(nodeIdentifier, "nodeIdentifier is null"));
    }

    public void retainActiveWorkers(Set<String> activeNodeIdentifiers)
    {
        readyWorkers.keySet().retainAll(requireNonNull(activeNodeIdentifiers, "activeNodeIdentifiers is null"));
    }

    public void allowAllWorkers()
    {
        readyWorkers.clear();
        readinessRequired = false;
    }
}
