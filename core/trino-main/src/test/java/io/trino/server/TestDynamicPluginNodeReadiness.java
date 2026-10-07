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

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamicPluginNodeReadiness
{
    @Test
    public void testUnknownWorkersAreExcludedUntilReconciled()
    {
        DynamicPluginNodeReadiness readiness = new DynamicPluginNodeReadiness();
        readiness.requireReadiness();
        assertThat(readiness.isWorkerReady("worker1", "instance-a")).isFalse();
        readiness.markReady("worker1", "instance-a");
        assertThat(readiness.isWorkerReady("worker1", "instance-a")).isTrue();
        assertThat(readiness.isWorkerReady("worker2", "instance-b")).isFalse();

        readiness.markNotReady("worker1");
        assertThat(readiness.isWorkerReady("worker1", "instance-a")).isFalse();
    }

    /**
     * A worker keeps its node id across a restart but runs as a new process. The readiness recorded for the
     * old process must not make the replacement schedulable.
     */
    @Test
    public void testRestartedWorkerDoesNotInheritReadiness()
    {
        DynamicPluginNodeReadiness readiness = new DynamicPluginNodeReadiness();
        readiness.requireReadiness();
        readiness.markReady("worker1", "instance-a");
        assertThat(readiness.isWorkerReady("worker1", "instance-a")).isTrue();

        assertThat(readiness.isWorkerReady("worker1", "instance-b")).isFalse();

        readiness.markReady("worker1", "instance-b");
        assertThat(readiness.isWorkerReady("worker1", "instance-b")).isTrue();
        assertThat(readiness.isWorkerReady("worker1", "instance-a")).isFalse();
    }

    @Test
    public void testInactiveWorkersLoseReadinessAndCanBeReconciledAgain()
    {
        DynamicPluginNodeReadiness readiness = new DynamicPluginNodeReadiness();
        readiness.requireReadiness();
        readiness.markReady("worker1", "instance-a");
        readiness.markReady("worker2", "instance-b");

        readiness.retainActiveWorkers(Set.of("worker2"));
        assertThat(readiness.isWorkerReady("worker1", "instance-a")).isFalse();
        assertThat(readiness.isWorkerReady("worker2", "instance-b")).isTrue();

        readiness.markReady("worker1", "instance-c");
        assertThat(readiness.isWorkerReady("worker1", "instance-c")).isTrue();
    }

    /**
     * The feature-off path must not depend on startup ordering: until readiness is required, every worker is
     * eligible, so a cluster that never enables dynamic plugins behaves exactly as before.
     */
    @Test
    public void testReadinessIsNotRequiredByDefault()
    {
        DynamicPluginNodeReadiness readiness = new DynamicPluginNodeReadiness();

        assertThat(readiness.isReadinessRequired()).isFalse();
        assertThat(readiness.isWorkerReady("any-worker", "any-instance")).isTrue();
    }

    @Test
    public void testAllowAllWorkersAfterFeatureDisabled()
    {
        DynamicPluginNodeReadiness readiness = new DynamicPluginNodeReadiness();
        readiness.requireReadiness();
        readiness.markReady("worker1", "instance-a");
        assertThat(readiness.isReadinessRequired()).isTrue();

        readiness.allowAllWorkers();

        assertThat(readiness.isReadinessRequired()).isFalse();
        assertThat(readiness.isWorkerReady("worker2", "instance-b")).isTrue();
    }
}
