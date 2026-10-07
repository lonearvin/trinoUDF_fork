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

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.json.JsonCodec;
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import io.trino.server.DynamicPluginNodeReadiness;
import io.trino.server.ServerInfo;
import io.trino.spi.NodeVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HttpStatus.OK;
import static io.trino.node.NodeState.ACTIVE;
import static io.trino.node.NodeState.INACTIVE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

class TestCoordinatorNodeManager
{
    private static final JsonCodec<ServerInfo> SERVER_INFO_JSON_CODEC = JsonCodec.jsonCodec(ServerInfo.class);
    private static final String EXPECTED_ENVIRONMENT = "test_1";

    private final Set<InternalNode> activeNodes;
    private final Set<InternalNode> inactiveNodes;
    private final Set<InternalNode> invalidNodes;
    private final InternalNode coordinator;
    private final InternalNode currentNode;
    private final TestingHttpClient testHttpClient;

    TestCoordinatorNodeManager()
    {
        NodeVersion expectedVersion = new NodeVersion("1");
        coordinator = new InternalNode(UUID.randomUUID().toString(), URI.create("https://192.0.2.8"), expectedVersion, true);
        currentNode = new InternalNode(UUID.randomUUID().toString(), URI.create("https://192.0.1.1"), expectedVersion, false);

        activeNodes = ImmutableSet.of(
                copy(currentNode),
                new InternalNode(UUID.randomUUID().toString(), URI.create("https://192.0.2.1:8080"), expectedVersion, false),
                new InternalNode(UUID.randomUUID().toString(), URI.create("https://192.0.2.3"), expectedVersion, false),
                coordinator);
        inactiveNodes = ImmutableSet.of(
                new InternalNode(UUID.randomUUID().toString(), URI.create("https://192.0.3.9"), expectedVersion, false),
                new InternalNode(UUID.randomUUID().toString(), URI.create("https://192.0.4.9"), expectedVersion, false));
        invalidNodes = ImmutableSet.of(
                new InternalNode("bad_version_unknown", URI.create("https://192.0.7.1"), NodeVersion.UNKNOWN, false),
                new InternalNode("bad_version_2", URI.create("https://192.0.7.2"), new NodeVersion("2"), false),
                new InternalNode("bad_environment", URI.create("https://192.0.7.3"), expectedVersion, false));

        ImmutableMap.Builder<String, ServerInfo> hostsBuilder = ImmutableMap.builder();
        for (InternalNode activeNode : activeNodes) {
            hostsBuilder.put(activeNode.getInternalUri().getHost(), toServerInfo(activeNode, ACTIVE, EXPECTED_ENVIRONMENT));
        }
        for (InternalNode inactiveNode : inactiveNodes) {
            hostsBuilder.put(inactiveNode.getInternalUri().getHost(), toServerInfo(inactiveNode, INACTIVE, EXPECTED_ENVIRONMENT));
        }
        for (InternalNode invalidNode : invalidNodes) {
            hostsBuilder.put(
                    invalidNode.getInternalUri().getHost(),
                    toServerInfo(invalidNode, INACTIVE, invalidNode.getNodeVersion().equals(expectedVersion) ? "bad_environment" : EXPECTED_ENVIRONMENT));
        }
        Map<String, ServerInfo> allHosts = hostsBuilder.buildOrThrow();

        testHttpClient = new TestingHttpClient(input -> {
            ServerInfo serverInfo = allHosts.get(input.getUri().getHost());
            if (serverInfo == null) {
                throw new IllegalArgumentException("Unknown host: " + input.getUri().getHost());
            }
            return new TestingResponse(
                    OK,
                    ImmutableListMultimap.of(CONTENT_TYPE, JSON_UTF_8.toString()),
                    SERVER_INFO_JSON_CODEC.toJsonBytes(serverInfo));
        });
    }

    @Test
    void testGetAllNodes()
    {
        CoordinatorNodeManager manager = new CoordinatorNodeManager(
                new TestingNodeInventory(),
                copy(currentNode),
                () -> ACTIVE,
                EXPECTED_ENVIRONMENT,
                testHttpClient,
                new TestingTicker());
        try {
            AllNodes allNodes = manager.getAllNodes();

            Set<InternalNode> connectorNodes = manager.getNodes(ACTIVE);
            assertThat(connectorNodes).hasSize(4);
            assertThat(connectorNodes.stream().anyMatch(InternalNode::isCoordinator)).isTrue();

            Set<InternalNode> activeNodes = allNodes.activeNodes();
            assertThat(activeNodes).containsExactlyInAnyOrderElementsOf(this.activeNodes);

            for (InternalNode actual : activeNodes) {
                for (InternalNode expected : this.activeNodes) {
                    assertThat(actual).isNotSameAs(expected);
                }
            }

            assertThat(activeNodes).containsExactlyInAnyOrderElementsOf(manager.getNodes(ACTIVE));

            Set<InternalNode> inactiveNodes = allNodes.inactiveNodes();
            assertThat(inactiveNodes).containsExactlyInAnyOrderElementsOf(this.inactiveNodes);

            for (InternalNode actual : inactiveNodes) {
                for (InternalNode expected : this.inactiveNodes) {
                    assertThat(actual).isNotSameAs(expected);
                }
            }

            assertThat(inactiveNodes).containsExactlyInAnyOrderElementsOf(manager.getNodes(INACTIVE));

            assertThat(manager.getInvalidNodes()).containsExactlyInAnyOrderElementsOf(invalidNodes);
        }
        finally {
            manager.stop();
        }
    }

    @Test
    void testActiveWorkersAreFilteredUntilPluginReadiness()
    {
        DynamicPluginNodeReadiness readiness = new DynamicPluginNodeReadiness();
        readiness.requireReadiness();
        CoordinatorNodeManager manager = new CoordinatorNodeManager(
                new TestingNodeInventory(),
                copy(currentNode),
                () -> ACTIVE,
                EXPECTED_ENVIRONMENT,
                readiness,
                testHttpClient,
                new TestingTicker());
        try {
            assertThat(manager.getAllNodes().activeNodes()).hasSize(4);
            assertThat(manager.getNodes(ACTIVE)).containsExactly(coordinator);
            assertThat(manager.getActiveNodesSnapshot().getAllNodes()).containsExactly(coordinator);

            String readyWorkerId = activeNodes.stream()
                    .filter(node -> !node.isCoordinator())
                    .map(InternalNode::getNodeIdentifier)
                    .filter(nodeId -> !nodeId.equals(currentNode.getNodeIdentifier()))
                    .findFirst()
                    .orElseThrow();

            // Readiness recorded for a different process of the same node must not make it schedulable
            readiness.markReady(readyWorkerId, "some-other-instance");
            assertThat(manager.getNodes(ACTIVE)).containsExactly(coordinator);

            String observedInstanceId = manager.getNodeInstanceId(readyWorkerId).orElseThrow();
            readiness.markReady(readyWorkerId, observedInstanceId);

            assertThat(manager.getNodes(ACTIVE))
                    .extracting(InternalNode::getNodeIdentifier)
                    .contains(readyWorkerId)
                    .doesNotContain(currentNode.getNodeIdentifier());
            assertThat(manager.getActiveNodesSnapshot().getAllNodes())
                    .extracting(InternalNode::getNodeIdentifier)
                    .contains(readyWorkerId);
        }
        finally {
            manager.stop();
        }
    }

    /**
     * A worker keeps its node id when it restarts, but it runs as a new process with a new instance id.
     * The readiness verified for the previous process must not keep the replacement schedulable, and the
     * worker must become schedulable again once the coordinator verifies the new process.
     */
    @Test
    void testRestartedWorkerLosesSchedulabilityUntilReconciled()
    {
        DynamicPluginNodeReadiness readiness = new DynamicPluginNodeReadiness();
        readiness.requireReadiness();

        InternalNode worker = activeNodes.stream()
                .filter(node -> !node.isCoordinator())
                .filter(node -> !node.getNodeIdentifier().equals(currentNode.getNodeIdentifier()))
                .findFirst()
                .orElseThrow();

        AtomicReference<String> instanceId = new AtomicReference<>("instance-1");
        Map<String, ServerInfo> otherHosts = new HashMap<>();
        for (InternalNode node : activeNodes) {
            if (!node.getNodeIdentifier().equals(worker.getNodeIdentifier())) {
                otherHosts.put(node.getInternalUri().getHost(), toServerInfo(node, ACTIVE, EXPECTED_ENVIRONMENT));
            }
        }
        TestingHttpClient client = new TestingHttpClient(input -> {
            String host = input.getUri().getHost();
            ServerInfo serverInfo = host.equals(worker.getInternalUri().getHost())
                    ? new ServerInfo(
                    worker.getNodeIdentifier(),
                    instanceId.get(),
                    ACTIVE,
                    worker.getNodeVersion(),
                    EXPECTED_ENVIRONMENT,
                    false,
                    Optional.empty(),
                    false,
                    Duration.ZERO)
                    : otherHosts.get(host);
            if (serverInfo == null) {
                throw new IllegalArgumentException("Unknown host: " + host);
            }
            return new TestingResponse(
                    OK,
                    ImmutableListMultimap.of(CONTENT_TYPE, JSON_UTF_8.toString()),
                    SERVER_INFO_JSON_CODEC.toJsonBytes(serverInfo));
        });

        TestingTicker ticker = new TestingTicker();
        CoordinatorNodeManager manager = new CoordinatorNodeManager(
                new TestingNodeInventory(),
                copy(currentNode),
                () -> ACTIVE,
                EXPECTED_ENVIRONMENT,
                readiness,
                client,
                ticker);
        try {
            // an unverified worker is not schedulable
            assertThat(manager.getNodes(ACTIVE)).doesNotContain(worker);

            readiness.markReady(worker.getNodeIdentifier(), "instance-1");
            assertThat(manager.getNodes(ACTIVE)).contains(worker);

            // the worker restarts with the same node id but a new process
            instanceId.set("instance-2");
            ticker.increment(5, SECONDS);
            manager.refreshNodes(true);

            assertThat(manager.getNodeInstanceId(worker.getNodeIdentifier())).contains("instance-2");
            assertThat(manager.getNodes(ACTIVE)).doesNotContain(worker);

            // once the coordinator has verified the new process, the worker is schedulable again
            readiness.markReady(worker.getNodeIdentifier(), "instance-2");
            assertThat(manager.getNodes(ACTIVE)).contains(worker);
        }
        finally {
            manager.stop();
        }
    }

    @Test
    void testWorkerWithoutReportedInstanceIdIsNotSchedulable()
    {
        DynamicPluginNodeReadiness readiness = new DynamicPluginNodeReadiness();
        readiness.requireReadiness();
        // The node manager cannot observe any process, so no worker's plugin state can be verified. The local
        // node of this fixture is a worker, so nothing is schedulable: the check must fail closed.
        CoordinatorNodeManager manager = new CoordinatorNodeManager(
                new TestingNodeInventory(),
                copy(currentNode),
                () -> ACTIVE,
                EXPECTED_ENVIRONMENT,
                readiness,
                new TestingHttpClient(_ -> {
                    throw new UnsupportedOperationException("no node state available");
                }),
                new TestingTicker());
        try {
            assertThat(manager.getAllNodes().activeNodes()).isNotEmpty();
            assertThat(manager.getNodes(ACTIVE)).isEmpty();
            assertThat(manager.getActiveNodesSnapshot().getAllNodes()).isEmpty();
        }
        finally {
            manager.stop();
        }
    }

    @Test
    void testGetCoordinators()
    {
        CoordinatorNodeManager manager = new CoordinatorNodeManager(
                new TestingNodeInventory(),
                copy(currentNode),
                () -> ACTIVE,
                EXPECTED_ENVIRONMENT,
                testHttpClient,
                new TestingTicker());
        try {
            assertThat(manager.getCoordinators()).isEqualTo(ImmutableSet.of(coordinator));
        }
        finally {
            manager.stop();
        }
    }

    @Test
    @Timeout(60)
    void testNodeChangeListener()
            throws Exception
    {
        // initially only the current node is announced
        TestingNodeInventory selector = new TestingNodeInventory();
        selector.announceNodes(ImmutableSet.of(currentNode));
        TestingTicker testingTicker = new TestingTicker();
        CoordinatorNodeManager manager = new CoordinatorNodeManager(
                selector,
                copy(currentNode),
                () -> ACTIVE,
                EXPECTED_ENVIRONMENT,
                testHttpClient,
                testingTicker);
        try {
            BlockingQueue<AllNodes> notifications = new ArrayBlockingQueue<>(100);
            manager.addNodeChangeListener(notifications::add);
            AllNodes allNodes = notifications.take();
            assertThat(manager.getAllNodes()).isSameAs(allNodes);
            assertThat(allNodes.activeNodes()).containsExactly(currentNode);
            assertThat(allNodes.inactiveNodes()).isEmpty();

            // announce all nodes
            testingTicker.increment(5, SECONDS);
            selector.announceNodes(activeNodes, inactiveNodes);
            manager.refreshNodes(true);
            allNodes = notifications.take();
            assertThat(manager.getAllNodes()).isSameAs(allNodes);
            assertThat(allNodes.activeNodes()).isEqualTo(activeNodes);
            assertThat(allNodes.activeCoordinators()).isEqualTo(ImmutableSet.of(coordinator));

            // only announce current node and inactive nodes
            // node manager tracks all nodes until they have not been seen for a while
            testingTicker.increment(30, SECONDS);
            selector.announceNodes(ImmutableSet.of(currentNode), inactiveNodes);
            manager.refreshNodes(true);
            allNodes = notifications.take();
            assertThat(manager.getAllNodes()).isSameAs(allNodes);
            assertThat(allNodes.activeNodes()).containsExactly(currentNode);
            assertThat(allNodes.inactiveNodes()).isEqualTo(inactiveNodes);
        }
        finally {
            manager.stop();
        }
    }

    private final class TestingNodeInventory
            implements NodeInventory
    {
        @GuardedBy("this")
        private Set<URI> nodes = ImmutableSet.of();

        public TestingNodeInventory()
        {
            announceNodes(activeNodes, inactiveNodes);
        }

        @SafeVarargs
        private synchronized void announceNodes(Set<InternalNode>... nodeSets)
        {
            ImmutableSet.Builder<URI> descriptors = ImmutableSet.builder();
            Stream.of(nodeSets)
                    .flatMap(Set::stream)
                    .map(InternalNode::getInternalUri)
                    .forEach(descriptors::add);

            // Add the invalid nodes
            invalidNodes.stream()
                    .map(InternalNode::getInternalUri)
                    .forEach(descriptors::add);

            this.nodes = descriptors.build();
        }

        @Override
        public synchronized Set<URI> getNodes()
        {
            return nodes;
        }
    }

    private static InternalNode copy(InternalNode node)
    {
        return new InternalNode(
                node.getNodeIdentifier(),
                node.getInternalUri(),
                node.getNodeVersion(),
                node.isCoordinator());
    }

    private static ServerInfo toServerInfo(InternalNode inactiveNode, NodeState nodeState, String environment)
    {
        return new ServerInfo(
                inactiveNode.getNodeIdentifier(),
                "instance-" + inactiveNode.getNodeIdentifier(),
                nodeState,
                inactiveNode.getNodeVersion(),
                environment,
                inactiveNode.isCoordinator(),
                Optional.empty(),
                false,
                Duration.ZERO);
    }
}
