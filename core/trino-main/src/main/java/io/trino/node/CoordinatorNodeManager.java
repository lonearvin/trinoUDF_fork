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
package io.trino.node;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.trino.server.DynamicPluginNodeReadiness;
import io.trino.server.NodeStateManager.CurrentNodeState;
import io.trino.spi.HostAddress;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.function.Function.identity;

@ThreadSafe
public final class CoordinatorNodeManager
        implements InternalNodeManager
{
    private static final Logger log = Logger.get(CoordinatorNodeManager.class);

    private final Supplier<NodeState> currentNodeState;
    private final NodeInventory nodeInventory;
    private final String expectedNodeEnvironment;
    private final ConcurrentHashMap<URI, RemoteNodeState> nodeStates = new ConcurrentHashMap<>();
    private final HttpClient httpClient;
    private final ScheduledExecutorService nodeStateUpdateExecutor;
    private final ExecutorService nodeStateEventExecutor;
    private final InternalNode currentNode;
    private final DynamicPluginNodeReadiness dynamicPluginNodeReadiness;
    private final Ticker ticker;

    @GuardedBy("this")
    private AllNodes allNodes;

    @GuardedBy("this")
    private Set<InternalNode> invalidNodes;

    @GuardedBy("this")
    private Map<String, String> nodeInstanceIds = Map.of();

    @GuardedBy("this")
    private Map<HostAddress, InternalNode> goneNodes;

    @GuardedBy("this")
    private final List<Consumer<AllNodes>> listeners = new ArrayList<>();

    @Inject
    public CoordinatorNodeManager(
            NodeInventory nodeInventory,
            NodeInfo nodeInfo,
            InternalNode currentNode,
            CurrentNodeState currentNodeState,
            DynamicPluginNodeReadiness dynamicPluginNodeReadiness,
            @ForNodeManager HttpClient httpClient)
    {
        this(nodeInventory,
                currentNode,
                currentNodeState,
                nodeInfo.getEnvironment(),
                dynamicPluginNodeReadiness,
                httpClient,
                Ticker.systemTicker());
    }

    @VisibleForTesting
    CoordinatorNodeManager(
            NodeInventory nodeInventory,
            InternalNode currentNode,
            Supplier<NodeState> currentNodeState,
            String expectedNodeEnvironment,
            HttpClient httpClient,
            Ticker ticker)
    {
        this(nodeInventory, currentNode, currentNodeState, expectedNodeEnvironment, permissiveReadiness(), httpClient, ticker);
    }

    private static DynamicPluginNodeReadiness permissiveReadiness()
    {
        DynamicPluginNodeReadiness readiness = new DynamicPluginNodeReadiness();
        readiness.allowAllWorkers();
        return readiness;
    }

    @VisibleForTesting
    CoordinatorNodeManager(
            NodeInventory nodeInventory,
            InternalNode currentNode,
            Supplier<NodeState> currentNodeState,
            String expectedNodeEnvironment,
            DynamicPluginNodeReadiness dynamicPluginNodeReadiness,
            HttpClient httpClient,
            Ticker ticker)
    {
        this.nodeInventory = requireNonNull(nodeInventory, "nodeInventory is null");
        this.currentNode = requireNonNull(currentNode, "currentNode is null");
        this.dynamicPluginNodeReadiness = requireNonNull(dynamicPluginNodeReadiness, "dynamicPluginNodeReadiness is null");
        this.currentNodeState = requireNonNull(currentNodeState, "currentNodeState is null");
        this.expectedNodeEnvironment = requireNonNull(expectedNodeEnvironment, "expectedNodeEnvironment is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.nodeStateUpdateExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("node-state-poller-%s"));
        this.nodeStateEventExecutor = newCachedThreadPool(daemonThreadsNamed("node-state-events-%s"));
        this.ticker = requireNonNull(ticker, "ticker is null");

        refreshNodes(false);
    }

    @PostConstruct
    public void startPollingNodeStates()
    {
        nodeStateUpdateExecutor.scheduleWithFixedDelay(() -> {
            try {
                refreshNodes(false);
            }
            catch (Exception e) {
                log.error(e, "Error polling state of nodes");
            }
        }, 5, 5, TimeUnit.SECONDS);
        refreshNodes(false);
    }

    @PreDestroy
    public void stop()
    {
        nodeStateUpdateExecutor.shutdown();
        nodeStateEventExecutor.shutdown();
    }

    @Override
    public boolean refreshNodes(boolean forceAndWait)
    {
        // Add new nodes
        for (URI uri : nodeInventory.getNodes()) {
            if (uri.equals(currentNode.getInternalUri())) {
                // Skip the current node
                continue;
            }

            // Mark the node as seen, and get the current state
            RemoteNodeState remoteNodeState = nodeStates.computeIfAbsent(
                    uri,
                    _ -> new RemoteNodeState(
                            uri,
                            expectedNodeEnvironment,
                            currentNode.getNodeVersion(),
                            httpClient,
                            ticker));
            remoteNodeState.setSeen();
        }

        // Remove nodes that are no longer present
        for (var entry : nodeStates.entrySet()) {
            RemoteNodeState remoteNodeState = entry.getValue();
            if (remoteNodeState.isMissing()) {
                if (remoteNodeState.hasBeenActive() && remoteNodeState.getState() != NodeState.SHUTTING_DOWN) {
                    log.info("Previously active node is missing: %s", entry.getKey());
                }
                nodeStates.remove(entry.getKey());
            }
        }

        // Schedule refresh
        ListenableFuture<List<Boolean>> future = Futures.allAsList(nodeStates.values().stream()
                .map(remoteNodeState -> remoteNodeState.asyncRefresh(forceAndWait))
                .toList());
        boolean result = true;
        if (forceAndWait) {
            result = getFutureValue(future).stream().allMatch(Boolean::booleanValue);
        }

        // update indexes
        refreshNodesInternal();

        return result;
    }

    private synchronized void refreshNodesInternal()
    {
        ImmutableSet.Builder<InternalNode> activeNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> inactiveNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> drainingNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> drainedNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> shuttingDownNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> invalidNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> goneNodesBuilder = ImmutableSet.builder();
        ImmutableMap.Builder<String, String> instanceIdsBuilder = ImmutableMap.builder();

        switch (currentNodeState.get()) {
            case ACTIVE -> activeNodesBuilder.add(currentNode);
            // INVALID or GONE should never happen, but if it does, treat as INACTIVE to avoid exceptions
            case INACTIVE, INVALID, GONE -> inactiveNodesBuilder.add(currentNode);
            case DRAINING -> drainingNodesBuilder.add(currentNode);
            case DRAINED -> drainedNodesBuilder.add(currentNode);
            case SHUTTING_DOWN -> shuttingDownNodesBuilder.add(currentNode);
        }

        for (RemoteNodeState remoteNodeState : nodeStates.values()) {
            InternalNode node = remoteNodeState.getInternalNode().orElse(null);
            if (node == null) {
                continue;
            }
            remoteNodeState.getNodeInstanceId().ifPresent(instanceId -> instanceIdsBuilder.put(node.getNodeIdentifier(), instanceId));
            switch (remoteNodeState.getState()) {
                case ACTIVE -> activeNodesBuilder.add(node);
                case INACTIVE -> inactiveNodesBuilder.add(node);
                case DRAINING -> drainingNodesBuilder.add(node);
                case DRAINED -> drainedNodesBuilder.add(node);
                case SHUTTING_DOWN -> shuttingDownNodesBuilder.add(node);
                case INVALID -> invalidNodesBuilder.add(node);
                case GONE -> goneNodesBuilder.add(node);
            }
        }

        this.invalidNodes = invalidNodesBuilder.build();
        this.nodeInstanceIds = instanceIdsBuilder.buildKeepingLast();
        this.goneNodes = goneNodesBuilder.build().stream()
                .collect(toImmutableMap(InternalNode::getHostAndPort, identity()));

        Set<InternalNode> activeNodes = activeNodesBuilder.build();
        Set<InternalNode> drainingNodes = drainingNodesBuilder.build();
        Set<InternalNode> drainedNodes = drainedNodesBuilder.build();
        Set<InternalNode> inactiveNodes = inactiveNodesBuilder.build();
        Set<InternalNode> shuttingDownNodes = shuttingDownNodesBuilder.build();

        Set<InternalNode> coordinators = activeNodes.stream()
                .filter(InternalNode::isCoordinator)
                .collect(toImmutableSet());

        AllNodes allNodes = new AllNodes(activeNodes, inactiveNodes, drainingNodes, drainedNodes, shuttingDownNodes, coordinators);
        // only update if all nodes actually changed (note: this does not include the connectors registered with the nodes)
        if (!allNodes.equals(this.allNodes)) {
            // assign allNodes to a local variable for use in the callback below
            this.allNodes = allNodes;

            // notify listeners
            List<Consumer<AllNodes>> listeners = ImmutableList.copyOf(this.listeners);
            nodeStateEventExecutor.submit(() -> listeners.forEach(listener -> listener.accept(allNodes)));
        }
    }

    @Override
    public Set<InternalNode> getNodes(NodeState state)
    {
        Set<InternalNode> nodes = InternalNodeManager.super.getNodes(state);
        if (state != NodeState.ACTIVE) {
            return nodes;
        }
        return nodes.stream()
                .filter(this::isSchedulable)
                .collect(toImmutableSet());
    }

    /**
     * A node may be selected for work when dynamic plugin readiness is disabled, when it is the coordinator,
     * or when the process currently running on it has been verified to hold the coordinator's plugin set.
     * The instance id check is what stops a restarted worker from reusing the readiness of its predecessor.
     */
    private boolean isSchedulable(InternalNode node)
    {
        if (node.isCoordinator()) {
            return true;
        }
        String instanceId = nodeInstanceIds.get(node.getNodeIdentifier());
        if (instanceId == null) {
            // The node has never reported an instance id, so its plugin state cannot be verified
            return !dynamicPluginNodeReadiness.isReadinessRequired();
        }
        return dynamicPluginNodeReadiness.isWorkerReady(node.getNodeIdentifier(), instanceId);
    }

    @Override
    public synchronized Optional<String> getNodeInstanceId(String nodeIdentifier)
    {
        return Optional.ofNullable(nodeInstanceIds.get(requireNonNull(nodeIdentifier, "nodeIdentifier is null")));
    }

    @Override
    public NodesSnapshot getActiveNodesSnapshot()
    {
        return new NodesSnapshot(getNodes(NodeState.ACTIVE));
    }

    @Override
    public synchronized AllNodes getAllNodes()
    {
        return allNodes;
    }

    @Managed
    public int getActiveNodeCount()
    {
        return getAllNodes().activeNodes().size();
    }

    @Managed
    public int getInactiveNodeCount()
    {
        return getAllNodes().inactiveNodes().size();
    }

    @Managed
    public int getDrainingNodeCount()
    {
        return getAllNodes().drainingNodes().size();
    }

    @Managed
    public int getDrainedNodeCount()
    {
        return getAllNodes().drainedNodes().size();
    }

    @Managed
    public int getShuttingDownNodeCount()
    {
        return getAllNodes().shuttingDownNodes().size();
    }

    @Managed
    public int getActiveCoordinatorCount()
    {
        return getAllNodes().activeCoordinators().size();
    }

    @Managed
    public int getActiveWorkerCount()
    {
        AllNodes allNodes = getAllNodes();
        return allNodes.activeNodes().size() - allNodes.activeCoordinators().size();
    }

    /**
     * Workers that may currently receive work. This is lower than {@link #getActiveWorkerCount()} while a
     * worker's dynamic plugin state has not been verified, or before a restarting worker is reconciled.
     */
    @Managed
    public int getSchedulableWorkerCount()
    {
        return (int) getNodes(NodeState.ACTIVE).stream()
                .filter(node -> !node.isCoordinator())
                .count();
    }

    @VisibleForTesting
    synchronized Set<InternalNode> getInvalidNodes()
    {
        return invalidNodes;
    }

    @Override
    public synchronized boolean isGone(HostAddress hostAddress)
    {
        requireNonNull(hostAddress, "hostAddress is null");
        return goneNodes.containsKey(hostAddress);
    }

    @Override
    public synchronized void addNodeChangeListener(Consumer<AllNodes> listener)
    {
        listeners.add(requireNonNull(listener, "listener is null"));
        AllNodes allNodes = this.allNodes;
        nodeStateEventExecutor.submit(() -> listener.accept(allNodes));
    }

    @Override
    public synchronized void removeNodeChangeListener(Consumer<AllNodes> listener)
    {
        listeners.remove(requireNonNull(listener, "listener is null"));
    }
}
