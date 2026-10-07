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

import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static java.util.Objects.requireNonNull;

/**
 * Coordinator-side admission barrier for dynamic plugin changes. Every dispatched query holds a lease
 * until it reaches a terminal state. An ADD JAR or DROP JAR closes admission, releases its own lease,
 * and drains all other queries before changing any plugin class loader.
 * <p>
 * The barrier starts closed only when dynamic plugins are enabled, so a cluster that does not use the
 * feature admits queries exactly as before, with no dependence on startup ordering.
 */
public final class DynamicPluginAdmission
{
    private final Set<QueryId> admittedQueries = new HashSet<>();

    private boolean updateInProgress;
    private boolean recovering;
    private Throwable inconsistentState;

    @Inject
    public DynamicPluginAdmission(DynamicPluginConfig config)
    {
        // When the feature is off there is nothing to protect, so admission is open from construction.
        // When it is on, start closed: plugin state is not known to be consistent until recovery completes.
        this.recovering = requireNonNull(config, "config is null").isEnabled();
    }

    public DynamicPluginAdmission()
    {
        this.recovering = false;
    }

    public synchronized boolean tryAdmit(QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");
        if (updateInProgress || recovering || inconsistentState != null) {
            return false;
        }
        return admittedQueries.add(queryId);
    }

    public synchronized void release(QueryId queryId)
    {
        if (admittedQueries.remove(requireNonNull(queryId, "queryId is null"))) {
            notifyAll();
        }
    }

    public synchronized UpdateLease beginUpdate(QueryId owner, Duration drainTimeout)
    {
        requireNonNull(owner, "owner is null");
        requireNonNull(drainTimeout, "drainTimeout is null");
        if (inconsistentState != null) {
            throw new TrinoException(QUERY_REJECTED, "Dynamic plugin state is inconsistent; query admission remains closed", inconsistentState);
        }
        if (recovering || updateInProgress) {
            throw new TrinoException(QUERY_REJECTED, "Another dynamic plugin operation or plugin recovery is in progress");
        }
        if (!admittedQueries.remove(owner)) {
            throw new TrinoException(QUERY_REJECTED, "Dynamic plugin update query does not hold an admission lease");
        }

        updateInProgress = true;
        long timeoutNanos = drainTimeout.toMillis() <= 0 ? 0 : TimeUnit.MILLISECONDS.toNanos(drainTimeout.toMillis());
        long deadline = System.nanoTime() + timeoutNanos;
        while (!admittedQueries.isEmpty()) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                updateInProgress = false;
                notifyAll();
                throw new TrinoException(QUERY_REJECTED,
                        "Dynamic plugin update aborted: %s query(s) did not finish within %s"
                                .formatted(admittedQueries.size(), drainTimeout));
            }
            try {
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                updateInProgress = false;
                notifyAll();
                throw new TrinoException(QUERY_REJECTED, "Interrupted while draining queries for dynamic plugin update", e);
            }
        }
        return new UpdateLease(this);
    }

    public synchronized boolean isQueryAdmissionOpen()
    {
        return !updateInProgress && !recovering && inconsistentState == null;
    }

    public synchronized void closeForRecovery()
    {
        recovering = true;
    }

    public synchronized void completeRecovery()
    {
        recovering = false;
        notifyAll();
    }

    public synchronized void failRecovery(Throwable cause)
    {
        recovering = false;
        inconsistentState = requireNonNull(cause, "cause is null");
        notifyAll();
    }

    private synchronized void finishUpdate(Throwable failure)
    {
        if (failure != null) {
            inconsistentState = failure;
        }
        updateInProgress = false;
        notifyAll();
    }

    public static final class UpdateLease
            implements AutoCloseable
    {
        private final DynamicPluginAdmission admission;
        private Throwable failure;
        private boolean closed;

        private UpdateLease(DynamicPluginAdmission admission)
        {
            this.admission = admission;
        }

        /**
         * Keep query admission closed when rollback or reconciliation could not restore a known-good
         * cluster state. A restart after repairing the persisted manifest is required to reopen it.
         */
        public void failClosed(Throwable cause)
        {
            failure = requireNonNull(cause, "cause is null");
        }

        @Override
        public void close()
        {
            if (!closed) {
                closed = true;
                admission.finishUpdate(failure);
            }
        }
    }
}
