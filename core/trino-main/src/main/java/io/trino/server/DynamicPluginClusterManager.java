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

import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.log.Logger;
import io.trino.node.InternalNode;
import io.trino.node.InternalNodeManager;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Coordinates dynamic plugin changes across the coordinator and all active workers.
 * Plugin artifacts are transferred as checksummed bytes and stored locally in a content-addressed cache.
 */
public class DynamicPluginClusterManager
{
    private static final Logger log = Logger.get(DynamicPluginClusterManager.class);

    private static final String MANIFEST_FILE = "manifest.properties";

    /**
     * Wait before restoring, so workers have time to register after a coordinator restart.
     */
    private static final long RESTORE_DELAY_SECONDS = 15;

    private final PluginManager pluginManager;
    private final InternalNodeManager nodeManager;
    private final HttpClient httpClient;
    private final Path jarDirectory;
    private final boolean coordinator;
    private final boolean enabled;
    private final DynamicPluginConfig config;
    private final DynamicPluginAdmission admission;
    private final DynamicPluginNodeReadiness nodeReadiness;

    private final AtomicBoolean syncStarted = new AtomicBoolean();
    private final ScheduledExecutorService syncExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("dynamic-plugin-sync"));

    @Inject
    public DynamicPluginClusterManager(PluginManager pluginManager, InternalNodeManager nodeManager, InternalNode currentNode, DynamicPluginConfig config, InternalCommunicationConfig internalCommunicationConfig, DynamicPluginAdmission admission, DynamicPluginNodeReadiness nodeReadiness, @ForDynamicPlugin HttpClient httpClient)
    {
        this.pluginManager = requireNonNull(pluginManager, "pluginManager is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.coordinator = requireNonNull(currentNode, "currentNode is null").isCoordinator();
        this.config = requireNonNull(config, "config is null");
        this.enabled = config.isEnabled();
        this.admission = requireNonNull(admission, "admission is null");
        this.nodeReadiness = requireNonNull(nodeReadiness, "nodeReadiness is null");
        this.jarDirectory = config.getJarDirectory().map(File::toPath).orElse(PluginArtifact.CACHE_DIRECTORY);

        // Loading a plugin JAR is remote code execution on every node. The worker endpoint that accepts JARs is
        // authenticated with the internal shared secret; when that secret is absent Trino falls back to a key
        // derived from the node environment, which is not a secret. Refuse to enable the feature in that case
        // rather than expose a code-loading endpoint protected by a guessable key.
        if (enabled && internalCommunicationConfig.getSharedSecret().isEmpty()) {
            throw new IllegalArgumentException(
                    "dynamic-plugin.enabled=true requires an explicit internal-communication.shared-secret. "
                            + "Without it, internal requests are authenticated with a key derived from the node environment, "
                            + "which would allow any host able to reach the internal port to load plugin code.");
        }
    }

    @PreDestroy
    public void stop()
    {
        syncExecutor.shutdownNow();
    }

    /**
     * Restores previously loaded plugins after a coordinator restart. Without this, every dynamically
     * loaded UDF would silently disappear when the coordinator comes back.
     */
    @PostConstruct
    public void start()
    {
        if (!coordinator) {
            return;
        }
        if (!enabled) {
            nodeReadiness.allowAllWorkers();
            admission.completeRecovery();
            return;
        }

        nodeReadiness.requireReadiness();
        admission.closeForRecovery();
        ensureSyncStarted();
        syncExecutor.schedule(() -> {
            try {
                if (restorePlugins()) {
                    admission.completeRecovery();
                    log.info("Dynamic plugin recovery completed; query admission is open");
                }
                else {
                    TrinoException failure = new TrinoException(GENERIC_INTERNAL_ERROR, "Dynamic plugin recovery was incomplete; query admission remains closed");
                    admission.failRecovery(failure);
                    log.error(failure, "Dynamic plugin recovery did not converge");
                }
            }
            catch (Throwable failure) {
                admission.failRecovery(failure);
                log.error(failure, "Dynamic plugin recovery failed; query admission remains closed");
            }
        }, RESTORE_DELAY_SECONDS, SECONDS);
    }

    public void addPlugin(QueryId queryId, String pluginName, byte[] artifact)
    {
        try (DynamicPluginAdmission.UpdateLease update = admission.beginUpdate(queryId, config.getQueryDrainTimeout())) {
            synchronized (this) {
                try {
                    addPluginInternal(pluginName, artifact, true);
                }
                catch (DynamicPluginInconsistentException e) {
                    update.failClosed(e);
                    throw e;
                }
            }
        }
    }

    private synchronized void addPluginInternal(String pluginName, byte[] artifact, boolean persistManifest)
    {
        requireNonNull(pluginName, "pluginName is null");
        if (!pluginName.matches("[a-z0-9][a-z0-9_.-]{0,127}")) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid plugin name: " + pluginName);
        }
        requireNonNull(artifact, "artifact is null");

        String checksum = PluginArtifact.checksum(artifact);
        URL localUrl = PluginArtifact.materialize(jarDirectory, checksum, artifact);
        List<InternalNode> workers = activeWorkers();
        ensureSyncStarted();

        Map<String, String> previousManifest = manifestSnapshot();
        String previousChecksum = previousManifest.get(pluginName);
        if (persistManifest) {
            Map<String, String> nextManifest = new HashMap<>(previousManifest);
            nextManifest.put(pluginName, checksum);
            writeManifest(nextManifest);
        }

        log.info("Adding plugin %s (sha256 %s) to %d worker(s)", pluginName, checksum, workers.size());
        try {
            workers.forEach(worker -> nodeReadiness.markNotReady(worker.getNodeIdentifier()));
            pluginManager.installPluginFromUrls(pluginName, List.of(localUrl), checksum);
            for (InternalNode worker : workers) {
                ensureSuccess(worker, addRequest(worker, pluginName, checksum, artifact));
            }
            reconcileWorkers(workers);
        }
        catch (RuntimeException failure) {
            RuntimeException rollbackFailure = null;
            try {
                if (persistManifest) {
                    writeManifest(previousManifest);
                }
                restorePlugin(pluginName, previousChecksum, workers);
                reconcileWorkers(workers);
            }
            catch (RuntimeException e) {
                failure.addSuppressed(e);
                rollbackFailure = e;
            }
            if (rollbackFailure != null) {
                workers.forEach(worker -> nodeReadiness.markNotReady(worker.getNodeIdentifier()));
                throw new DynamicPluginInconsistentException("Failed to load plugin %s and rollback did not restore cluster consistency".formatted(pluginName), failure);
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to load plugin %s on every active worker".formatted(pluginName), failure);
        }
    }

    public void dropPlugin(QueryId queryId, String pluginName)
    {
        try (DynamicPluginAdmission.UpdateLease update = admission.beginUpdate(queryId, config.getQueryDrainTimeout())) {
            synchronized (this) {
                try {
                    dropPluginInternal(pluginName);
                }
                catch (DynamicPluginInconsistentException e) {
                    update.failClosed(e);
                    throw e;
                }
            }
        }
    }

    private synchronized void dropPluginInternal(String pluginName)
    {
        requireNonNull(pluginName, "pluginName is null");
        if (!pluginName.matches("[a-z0-9][a-z0-9_.-]{0,127}")) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid plugin name: " + pluginName);
        }
        List<InternalNode> workers = activeWorkers();
        Map<String, String> previousManifest = manifestSnapshot();
        String checksum = previousManifest.get(pluginName);
        if (checksum == null) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Plugin is not loaded: " + pluginName);
        }

        byte[] artifact;
        try {
            artifact = Files.readAllBytes(jarDirectory.resolve(checksum + ".jar"));
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Cannot read JAR for plugin rollback: " + pluginName, e);
        }

        Map<String, String> nextManifest = new HashMap<>(previousManifest);
        nextManifest.remove(pluginName);
        writeManifest(nextManifest);

        try {
            workers.forEach(worker -> nodeReadiness.markNotReady(worker.getNodeIdentifier()));
            for (InternalNode worker : workers) {
                ensureSuccess(worker, dropRequest(worker, pluginName));
            }
            if (!pluginManager.uninstallPluginIfPresent(pluginName)) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Plugin is not loaded locally: " + pluginName);
            }
            reconcileWorkers(workers);
        }
        catch (RuntimeException failure) {
            RuntimeException rollbackFailure = null;
            try {
                writeManifest(previousManifest);
                URL oldUrl = PluginArtifact.materialize(jarDirectory, checksum, artifact);
                pluginManager.installPluginFromUrls(pluginName, List.of(oldUrl), checksum);
                for (InternalNode worker : workers) {
                    pushTo(worker, pluginName, checksum, artifact);
                }
                reconcileWorkers(workers);
            }
            catch (RuntimeException e) {
                failure.addSuppressed(e);
                rollbackFailure = e;
            }
            if (rollbackFailure != null) {
                workers.forEach(worker -> nodeReadiness.markNotReady(worker.getNodeIdentifier()));
                throw new DynamicPluginInconsistentException("Failed to drop plugin %s and rollback did not restore cluster consistency".formatted(pluginName), failure);
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to drop plugin %s on every active worker".formatted(pluginName), failure);
        }
    }

    /**
     * Brings newly joined workers up to date, so a node that was added or restarted after a plugin was
     * loaded does not silently lack its functions.
     * <p>
     * This polls on a timer instead of registering a node change listener: registering a listener during
     * Guice initialization deadlocks against the node manager. It starts lazily, so a node that never
     * loads a plugin pays nothing.
     */
    private void ensureSyncStarted()
    {
        if (syncStarted.compareAndSet(false, true)) {
            syncExecutor.scheduleWithFixedDelay(this::syncNewWorkers, 10, 10, SECONDS);
        }
    }

    private synchronized void syncNewWorkers()
    {
        if (!admission.isQueryAdmissionOpen()) {
            return;
        }
        List<InternalNode> workers = activeWorkers();
        nodeReadiness.retainActiveWorkers(workers.stream()
                .map(InternalNode::getNodeIdentifier)
                .collect(java.util.stream.Collectors.toUnmodifiableSet()));
        for (InternalNode worker : workers) {
            try {
                if (!reconcileWorker(worker)) {
                    nodeReadiness.markNotReady(worker.getNodeIdentifier());
                    log.warn("Worker %s does not yet match the coordinator dynamic plugin set", worker.getNodeIdentifier());
                }
            }
            catch (IOException | RuntimeException e) {
                nodeReadiness.markNotReady(worker.getNodeIdentifier());
                log.warn(e, "Failed to reconcile dynamic plugins on worker %s", worker.getNodeIdentifier());
            }
        }
    }

    private void reconcileWorkers(List<InternalNode> workers)
    {
        for (InternalNode worker : workers) {
            try {
                if (!reconcileWorker(worker)) {
                    throw new TrinoException(
                            GENERIC_INTERNAL_ERROR,
                            "Worker %s did not converge to the coordinator dynamic plugin set".formatted(worker.getNodeIdentifier()));
                }
            }
            catch (IOException | RuntimeException e) {
                nodeReadiness.markNotReady(worker.getNodeIdentifier());
                throw new TrinoException(
                        GENERIC_INTERNAL_ERROR,
                        "Failed to verify dynamic plugin readiness on worker %s".formatted(worker.getNodeIdentifier()),
                        e);
            }
        }
    }

    private boolean reconcileWorker(InternalNode worker)
            throws IOException
    {
        Map<String, String> expected = manifestSnapshot();
        RemotePluginState state = getRemotePluginState(worker);
        if (state.plugins().equals(expected)) {
            return markReadyIfCurrentProcess(worker, state);
        }

        nodeReadiness.markNotReady(worker.getNodeIdentifier());
        for (String remotePlugin : state.plugins().keySet()) {
            if (!expected.containsKey(remotePlugin)) {
                ensureSuccess(worker, dropRequest(worker, remotePlugin));
            }
        }
        for (Map.Entry<String, String> plugin : expected.entrySet()) {
            if (plugin.getValue().equals(state.plugins().get(plugin.getKey()))) {
                continue;
            }
            byte[] artifact = Files.readAllBytes(jarDirectory.resolve(plugin.getValue() + ".jar"));
            if (!PluginArtifact.checksum(artifact).equals(plugin.getValue())) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Checksum mismatch for local plugin JAR " + plugin.getKey());
            }
            pushTo(worker, plugin.getKey(), plugin.getValue(), artifact);
        }

        RemotePluginState verified = getRemotePluginState(worker);
        if (!verified.plugins().equals(expected)) {
            return false;
        }
        return markReadyIfCurrentProcess(worker, verified);
    }

    /**
     * Records readiness only for the process the coordinator currently observes. The plugin state response
     * carries the instance id of the process that served it, so a response that raced with a worker restart
     * cannot mark the replacement process ready.
     */
    private boolean markReadyIfCurrentProcess(InternalNode worker, RemotePluginState state)
    {
        String observedInstanceId = nodeManager.getNodeInstanceId(worker.getNodeIdentifier()).orElse(null);
        if (observedInstanceId == null) {
            // The node manager has not observed this node's process yet, so nothing can be verified
            nodeReadiness.markNotReady(worker.getNodeIdentifier());
            return false;
        }
        if (!observedInstanceId.equals(state.instanceId())) {
            log.info("Ignoring stale plugin state from worker %s: serving process %s, observed process %s",
                    worker.getNodeIdentifier(),
                    state.instanceId(),
                    observedInstanceId);
            nodeReadiness.markNotReady(worker.getNodeIdentifier());
            return false;
        }
        nodeReadiness.markReady(worker.getNodeIdentifier(), state.instanceId());
        return true;
    }

    private RemotePluginState getRemotePluginState(InternalNode node)
    {
        Request request = prepareGet()
                .setUri(uriBuilderFrom(node.getInternalUri()).appendPath("v1/plugin").build())
                .build();
        StringResponse response = Futures.getUnchecked(httpClient.executeAsync(request, createStringResponseHandler()));
        if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Failed to query plugin state on node %s: HTTP %s".formatted(node.getNodeIdentifier(), response.getStatusCode()));
        }
        String body = response.getBody();
        if (body == null || body.isEmpty()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Empty plugin state response from node " + node.getNodeIdentifier());
        }
        String instanceId = null;
        Map<String, String> plugins = new HashMap<>();
        for (String line : body.split("\\n")) {
            int separator = line.indexOf('=');
            if (separator <= 0) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid plugin state response from node " + node.getNodeIdentifier());
            }
            String name = line.substring(0, separator);
            String value = line.substring(separator + 1);
            if (name.equals("instance")) {
                instanceId = value;
                continue;
            }
            if (!name.matches("[a-z0-9][a-z0-9_.-]{0,127}") || !value.matches("[0-9a-f]{64}")) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid plugin state response from node " + node.getNodeIdentifier());
            }
            plugins.put(name, value);
        }
        if (instanceId == null || instanceId.isEmpty()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Plugin state response from node %s is missing the process instance id".formatted(node.getNodeIdentifier()));
        }
        return new RemotePluginState(instanceId, Map.copyOf(plugins));
    }

    private record RemotePluginState(String instanceId, Map<String, String> plugins) {}

    private boolean restorePlugins()
    {
        Path manifest = jarDirectory.resolve(MANIFEST_FILE);
        Map<String, String> recorded;
        if (!Files.isRegularFile(manifest)) {
            recorded = Map.of();
        }
        else {
            try {
                recorded = readManifest(manifest);
            }
            catch (IOException e) {
                log.error(e, "Failed to read dynamic plugin manifest %s", manifest);
                return false;
            }
        }
        boolean success = true;
        for (Map.Entry<String, String> entry : recorded.entrySet()) {
            String name = entry.getKey();
            String checksum = entry.getValue();
            if (!checksum.matches("[0-9a-f]{64}")) {
                log.error("Invalid checksum in dynamic plugin manifest for %s", name);
                success = false;
                continue;
            }
            if (!name.matches("[a-z0-9][a-z0-9_.-]{0,127}")) {
                log.error("Invalid plugin name in dynamic plugin manifest: %s", name);
                success = false;
                continue;
            }
            Path jar = jarDirectory.resolve(checksum + ".jar");
            if (!Files.isRegularFile(jar)) {
                log.error("Plugin %s is recorded in the manifest but its JAR %s is missing", name, jar);
                success = false;
                continue;
            }
            try {
                if (Files.size(jar) > config.getMaxJarSize().toBytes()) {
                    log.error("Plugin %s exceeds the configured size limit at %s", name, jar);
                    success = false;
                    continue;
                }
                byte[] artifact = Files.readAllBytes(jar);
                if (!PluginArtifact.checksum(artifact).equals(checksum)) {
                    log.error("Checksum mismatch for plugin %s at %s", name, jar);
                    success = false;
                    continue;
                }
                addPluginInternal(name, artifact, false);
                log.info("Restored plugin %s", name);
            }
            catch (IOException | RuntimeException e) {
                log.error(e, "Failed to restore plugin %s; it remains unavailable", name);
                success = false;
            }
        }
        if (!success) {
            return false;
        }
        try {
            reconcileWorkers(activeWorkers());
            return true;
        }
        catch (RuntimeException e) {
            log.error(e, "Failed to verify worker plugin state during recovery");
            return false;
        }
    }

    private Map<String, String> readManifest(Path manifest)
            throws IOException
    {
        Properties recorded = new Properties();
        try (InputStream in = Files.newInputStream(manifest)) {
            recorded.load(in);
        }
        Map<String, String> plugins = new HashMap<>();
        for (String name : recorded.stringPropertyNames()) {
            plugins.put(name, recorded.getProperty(name));
        }
        return plugins;
    }

    private Map<String, String> manifestSnapshot()
    {
        Map<String, String> plugins = new HashMap<>();
        for (PluginManager.LoadedPlugin plugin : pluginManager.listDynamicPlugins()) {
            plugins.put(plugin.name(), plugin.checksum());
        }
        return plugins;
    }

    private synchronized void writeManifest(Map<String, String> plugins)
    {
        Path manifest = jarDirectory.resolve(MANIFEST_FILE);
        Path staging = null;
        try {
            Files.createDirectories(jarDirectory);
            staging = Files.createTempFile(jarDirectory, "manifest-", ".tmp");
            Properties recorded = new Properties();
            recorded.putAll(plugins);
            try (OutputStream out = Files.newOutputStream(staging)) {
                recorded.store(out, "Dynamically loaded plugins. Managed by ADD JAR / DROP JAR; do not edit by hand.");
            }
            try {
                Files.move(staging, manifest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            }
            catch (AtomicMoveNotSupportedException e) {
                Files.move(staging, manifest, StandardCopyOption.REPLACE_EXISTING);
            }
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to persist dynamic plugin manifest " + manifest, e);
        }
        finally {
            if (staging != null) {
                try {
                    Files.deleteIfExists(staging);
                }
                catch (IOException ignored) {
                    log.warn("Failed to remove temporary plugin manifest %s", staging);
                }
            }
        }
    }

    private void restorePlugin(String pluginName, String checksum, List<InternalNode> workers)
    {
        if (checksum == null) {
            for (InternalNode worker : workers) {
                ensureSuccess(worker, dropRequest(worker, pluginName));
            }
            pluginManager.uninstallPluginIfPresent(pluginName);
            return;
        }

        Path jar = jarDirectory.resolve(checksum + ".jar");
        byte[] artifact;
        try {
            artifact = Files.readAllBytes(jar);
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Cannot restore previous plugin JAR " + jar, e);
        }
        URL url = PluginArtifact.materialize(jarDirectory, checksum, artifact);
        pluginManager.installPluginFromUrls(pluginName, List.of(url), checksum);
        for (InternalNode worker : workers) {
            pushTo(worker, pluginName, checksum, artifact);
        }
    }

    /**
     * Pushes the given plugin to a single node, used when a worker joins after the plugin was loaded.
     */
    public void pushTo(InternalNode node, String pluginName, String checksum, byte[] artifact)
    {
        ensureSuccess(node, addRequest(node, pluginName, checksum, artifact));
    }

    private List<InternalNode> activeWorkers()
    {
        return nodeManager.getAllNodes().activeNodes().stream()
                .filter(node -> !node.isCoordinator())
                .toList();
    }

    private Request addRequest(InternalNode node, String pluginName, String checksum, byte[] artifact)
    {
        return preparePost()
                .setUri(uriBuilderFrom(node.getInternalUri())
                        .appendPath("v1/plugin")
                        .appendPath(pluginName)
                        .addParameter("checksum", checksum)
                        .build())
                .setBodyGenerator(createStaticBodyGenerator(artifact))
                .setHeader(CONTENT_TYPE, "application/java-archive")
                .build();
    }

    private Request dropRequest(InternalNode node, String pluginName)
    {
        return prepareDelete()
                .setUri(uriBuilderFrom(node.getInternalUri()).appendPath("v1/plugin").appendPath(pluginName).build())
                .build();
    }

    private static final class DynamicPluginInconsistentException
            extends TrinoException
    {
        private DynamicPluginInconsistentException(String message, Throwable cause)
        {
            super(GENERIC_INTERNAL_ERROR, message, cause);
        }
    }

    private void ensureSuccess(InternalNode node, Request request)
    {
        StatusResponse response = Futures.getUnchecked(httpClient.executeAsync(request, createStatusResponseHandler()));
        if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Plugin operation failed on node %s: HTTP %s".formatted(node.getNodeIdentifier(), response.getStatusCode()));
        }
    }
}
