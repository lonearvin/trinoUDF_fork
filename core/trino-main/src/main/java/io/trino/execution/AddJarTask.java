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
package io.trino.execution;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.execution.warnings.WarningCollector;
import io.trino.security.AccessControl;
import io.trino.server.DynamicPluginClusterManager;
import io.trino.server.DynamicPluginConfig;
import io.trino.server.PluginArtifact;
import io.trino.spi.TrinoException;
import io.trino.sql.tree.AddJar;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Locale;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.util.Objects.requireNonNull;

/**
 * Executes {@code ADD JAR 'location'}: loads a plugin JAR into the running cluster and registers the
 * functions it provides, without restarting the cluster.
 */
public class AddJarTask
        implements DataDefinitionTask<AddJar>
{
    private static final Logger log = Logger.get(AddJarTask.class);

    private final DynamicPluginClusterManager dynamicPluginClusterManager;
    private final AccessControl accessControl;
    private final DynamicPluginConfig config;

    @Inject
    public AddJarTask(DynamicPluginClusterManager dynamicPluginClusterManager, AccessControl accessControl, DynamicPluginConfig config)
    {
        this.dynamicPluginClusterManager = requireNonNull(dynamicPluginClusterManager, "dynamicPluginClusterManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public String getName()
    {
        return "ADD JAR";
    }

    @Override
    public ListenableFuture<Void> execute(
            AddJar statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        String location = statement.getPath().getValue();
        String name = pluginName(location).toLowerCase(Locale.ENGLISH);

        if (!config.isEnabled()) {
            throw new TrinoException(PERMISSION_DENIED, "ADD JAR is disabled on this cluster; set dynamic-plugin.enabled=true to allow loading plugin JARs at runtime");
        }

        // Loading a JAR executes arbitrary Java code on every node, so it requires the privilege that
        // governs writing system information: administrators only by default.
        accessControl.checkCanWriteSystemInformation(stateMachine.getSession().getIdentity());

        byte[] artifact = PluginArtifact.read(location, config.getMaxJarSize().toBytes());
        log.info("ADD JAR requested: plugin=%s sha256=%s user=%s queryId=%s",
                name,
                PluginArtifact.checksum(artifact),
                stateMachine.getSession().getIdentity().getUser(),
                stateMachine.getSession().getQueryId());

        try {
            dynamicPluginClusterManager.addPlugin(stateMachine.getSession().getQueryId(), name, artifact);
        }
        catch (RuntimeException e) {
            log.error(
                    e,
                    "ADD JAR failed: plugin=%s sha256=%s user=%s queryId=%s",
                    name,
                    PluginArtifact.checksum(artifact),
                    stateMachine.getSession().getIdentity().getUser(),
                    stateMachine.getSession().getQueryId());
            throw e;
        }
        log.info("ADD JAR succeeded: plugin=%s sha256=%s user=%s queryId=%s",
                name,
                PluginArtifact.checksum(artifact),
                stateMachine.getSession().getIdentity().getUser(),
                stateMachine.getSession().getQueryId());
        return immediateVoidFuture();
    }

    /**
     * Derives the plugin name from the JAR location. The name is what {@code DROP JAR} takes, so it
     * must stay stable across reloads of the same JAR.
     */
    private static String pluginName(String location)
    {
        String name = location;
        int lastSlash = name.lastIndexOf('/');
        if (lastSlash >= 0) {
            name = name.substring(lastSlash + 1);
        }
        if (name.endsWith(".jar")) {
            name = name.substring(0, name.length() - ".jar".length());
        }
        checkArgument(name.matches("[a-z0-9][a-z0-9_.-]{0,127}"), "Invalid plugin name in JAR location: %s", location);
        return name;
    }
}
