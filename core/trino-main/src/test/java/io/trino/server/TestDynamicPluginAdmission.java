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

import io.airlift.units.Duration;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDynamicPluginAdmission
{
    @Test
    public void testUpdateClosesAdmissionAndWaitsForQueries()
            throws Exception
    {
        DynamicPluginAdmission admission = new DynamicPluginAdmission();
        admission.completeRecovery();
        QueryId updateQuery = new QueryId("update1");
        QueryId activeQuery = new QueryId("active1");
        QueryId newQuery = new QueryId("newquery1");

        assertThat(admission.tryAdmit(updateQuery)).isTrue();
        assertThat(admission.tryAdmit(activeQuery)).isTrue();

        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            Future<DynamicPluginAdmission.UpdateLease> update = executor.submit(
                    () -> admission.beginUpdate(updateQuery, new Duration(5, TimeUnit.SECONDS)));

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (admission.isQueryAdmissionOpen() && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(admission.isQueryAdmissionOpen()).isFalse();
            assertThat(admission.tryAdmit(newQuery)).isFalse();
            assertThat(update.isDone()).isFalse();

            admission.release(activeQuery);
            try (DynamicPluginAdmission.UpdateLease ignored = update.get(5, TimeUnit.SECONDS)) {
                assertThat(admission.tryAdmit(newQuery)).isFalse();
            }

            assertThat(admission.tryAdmit(newQuery)).isTrue();
            admission.release(newQuery);
        }
    }

    @Test
    public void testDrainTimeoutReopensAdmissionWithoutMutation()
    {
        DynamicPluginAdmission admission = new DynamicPluginAdmission();
        admission.completeRecovery();
        QueryId updateQuery = new QueryId("update2");
        QueryId activeQuery = new QueryId("active2");
        assertThat(admission.tryAdmit(updateQuery)).isTrue();
        assertThat(admission.tryAdmit(activeQuery)).isTrue();

        assertThatThrownBy(() -> admission.beginUpdate(updateQuery, new Duration(1, TimeUnit.MILLISECONDS)))
                .isInstanceOf(TrinoException.class);
        assertThat(admission.isQueryAdmissionOpen()).isTrue();
        assertThat(admission.tryAdmit(new QueryId("aftertimeout"))).isTrue();
    }

    @Test
    public void testInconsistentStateFailsClosed()
    {
        DynamicPluginAdmission admission = new DynamicPluginAdmission();
        admission.completeRecovery();
        QueryId updateQuery = new QueryId("update3");
        assertThat(admission.tryAdmit(updateQuery)).isTrue();

        try (DynamicPluginAdmission.UpdateLease update = admission.beginUpdate(updateQuery, new Duration(1, TimeUnit.SECONDS))) {
            update.failClosed(new IllegalStateException("rollback failed"));
        }

        assertThat(admission.isQueryAdmissionOpen()).isFalse();
        assertThat(admission.tryAdmit(new QueryId("afterfailure"))).isFalse();
    }
}
