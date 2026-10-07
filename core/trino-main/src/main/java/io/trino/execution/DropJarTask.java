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
import io.trino.spi.TrinoException;
import io.trino.sql.tree.DropJar;
import io.trino.sql.tree.Expression;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Executes {@code DROP JAR name}: unloads a plugin previously added with {@code ADD JAR}, together with
 * every function it registered.
 */
public class DropJarTask
        implements DataDefinitionTask<DropJar>
{
    private static final Logger log = Logger.get(DropJarTask.class);

    private final DynamicPluginClusterManager dynamicPluginClusterManager;
    private final AccessControl accessControl;
    private final DynamicPluginConfig config;

    @Inject
    public DropJarTask(DynamicPluginClusterManager dynamicPluginClusterManager, AccessControl accessControl, DynamicPluginConfig config)
    {
        this.dynamicPluginClusterManager = requireNonNull(dynamicPluginClusterManager, "dynamicPluginClusterManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public String getName()
    {
        return "DROP JAR";
    }

    @Override
    public ListenableFuture<Void> execute(
            DropJar statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        String name = statement.getName().getValue().toLowerCase(ENGLISH);

        if (!config.isEnabled()) {
            throw new TrinoException(PERMISSION_DENIED, "DROP JAR is disabled on this cluster; set dynamic-plugin.enabled=true to manage plugin JARs at runtime");
        }

        accessControl.checkCanWriteSystemInformation(stateMachine.getSession().getIdentity());

        try {
            dynamicPluginClusterManager.dropPlugin(stateMachine.getSession().getQueryId(), name);
        }
        catch (RuntimeException e) {
            log.error(
                    e,
                    "DROP JAR failed: plugin=%s user=%s queryId=%s",
                    name,
                    stateMachine.getSession().getIdentity().getUser(),
                    stateMachine.getSession().getQueryId());
            throw e;
        }
        log.info("DROP JAR succeeded: plugin=%s user=%s queryId=%s",
                name,
                stateMachine.getSession().getIdentity().getUser(),
                stateMachine.getSession().getQueryId());
        return immediateVoidFuture();
    }
}
