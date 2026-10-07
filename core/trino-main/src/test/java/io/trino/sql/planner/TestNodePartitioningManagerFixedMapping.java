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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSelector;
import io.trino.execution.scheduler.NodeSelectorFactory;
import io.trino.node.AllNodes;
import io.trino.node.InternalNode;
import io.trino.node.InternalNodeManager;
import io.trino.spi.HostAddress;
import io.trino.spi.NodeVersion;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A connector can supply a fixed bucket-to-node mapping, and that mapping is used directly as the set of
 * nodes a stage runs on. It must therefore be validated against the nodes that are currently allowed to
 * receive work, or a stage could be dispatched to a node that has been excluded from scheduling.
 */
public class TestNodePartitioningManagerFixedMapping
{
    private static final NodeVersion VERSION = new NodeVersion("test");

    @Test
    public void testFixedMappingRejectedWhenNodeIsNotSchedulable()
    {
        InternalNode schedulable = node("schedulable");
        InternalNode excluded = node("excluded");
        NodePartitioningManager manager = managerWithSchedulableNodes(ImmutableSet.of(schedulable));

        // a mapping that only uses schedulable nodes is accepted
        manager.checkFixedMappingSchedulable(ImmutableList.of(schedulable));

        // a mapping that includes an excluded node must be rejected rather than dispatched
        assertThatThrownBy(() -> manager.checkFixedMappingSchedulable(ImmutableList.of(schedulable, excluded)))
                .isInstanceOf(TrinoException.class)
                .satisfies(exception -> assertThat(((TrinoException) exception).getErrorCode())
                        .isEqualTo(StandardErrorCode.NO_NODES_AVAILABLE.toErrorCode()))
                .hasMessageContaining("excluded");
    }

    @Test
    public void testFixedMappingRejectedWhenNoNodeIsSchedulable()
    {
        NodePartitioningManager manager = managerWithSchedulableNodes(ImmutableSet.of());

        assertThatThrownBy(() -> manager.checkFixedMappingSchedulable(ImmutableList.of(node("worker1"))))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("worker1");
    }

    private static NodePartitioningManager managerWithSchedulableNodes(Set<InternalNode> schedulableNodes)
    {
        InternalNodeManager nodeManager = new StubNodeManager(schedulableNodes);
        NodeSelectorFactory selectorFactory = new NodeSelectorFactory()
        {
            @Override
            public NodeSelector createNodeSelector(Session session)
            {
                throw new UnsupportedOperationException("not used by this test");
            }
        };
        return new NodePartitioningManager(new NodeScheduler(selectorFactory), CatalogServiceProvider.fail(), nodeManager);
    }

    private static InternalNode node(String identifier)
    {
        return new InternalNode(identifier, URI.create("https://192.0.2.1:8080"), VERSION, false);
    }

    private static class StubNodeManager
            implements InternalNodeManager
    {
        private final Set<InternalNode> schedulableNodes;

        private StubNodeManager(Set<InternalNode> schedulableNodes)
        {
            this.schedulableNodes = ImmutableSet.copyOf(schedulableNodes);
        }

        @Override
        public AllNodes getAllNodes()
        {
            // the coordinator is always schedulable, so model an empty set of remaining nodes
            return new AllNodes(schedulableNodes, ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of());
        }

        @Override
        public boolean isGone(HostAddress hostAddress)
        {
            return false;
        }

        @Override
        public boolean refreshNodes(boolean forceAndWait)
        {
            return true;
        }

        @Override
        public void addNodeChangeListener(Consumer<AllNodes> listener) {}

        @Override
        public void removeNodeChangeListener(Consumer<AllNodes> listener) {}
    }
}
