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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Configuration for loading plugin JARs at runtime with {@code ADD JAR}.
 * <p>
 * Disabled by default: {@code ADD JAR} executes arbitrary Java code on every node, so it must be
 * switched on deliberately. The default system access control denies {@code checkCanWriteSystemInformation}; an explicit
 * system access control policy must grant it to trusted administrators before the SQL statement can run.
 */
public class DynamicPluginConfig
{
    private boolean enabled;
    private Optional<File> jarDirectory = Optional.empty();
    private DataSize maxJarSize = DataSize.of(64, MEGABYTE);
    private Duration workerTimeout = new Duration(30, MINUTES);
    private Duration queryDrainTimeout = new Duration(5, MINUTES);

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("dynamic-plugin.enabled")
    @ConfigDescription("Allow loading plugin JARs at runtime with ADD JAR; disabled by default because it executes arbitrary code")
    public DynamicPluginConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @NotNull
    public Optional<File> getJarDirectory()
    {
        return jarDirectory;
    }

    @Config("dynamic-plugin.jar-directory")
    @ConfigDescription("Directory where content-addressed plugin JARs and the active plugin manifest are stored; defaults to ~/.trino/dynamic-plugins")
    public DynamicPluginConfig setJarDirectory(File jarDirectory)
    {
        this.jarDirectory = Optional.ofNullable(jarDirectory);
        return this;
    }

    @NotNull
    public DataSize getMaxJarSize()
    {
        return maxJarSize;
    }

    @Config("dynamic-plugin.max-jar-size")
    @ConfigDescription("Maximum accepted size of a single plugin JAR")
    public DynamicPluginConfig setMaxJarSize(DataSize maxJarSize)
    {
        this.maxJarSize = maxJarSize;
        return this;
    }

    @NotNull
    public Duration getQueryDrainTimeout()
    {
        return queryDrainTimeout;
    }

    @Config("dynamic-plugin.query-drain-timeout")
    @ConfigDescription("Maximum time to wait for in-flight queries to finish before an ADD JAR or DROP JAR is rejected")
    public DynamicPluginConfig setQueryDrainTimeout(Duration queryDrainTimeout)
    {
        this.queryDrainTimeout = queryDrainTimeout;
        return this;
    }

    @NotNull
    public Duration getWorkerTimeout()
    {
        return workerTimeout;
    }

    @Config("dynamic-plugin.worker-timeout")
    @ConfigDescription("How long to wait for a worker to acknowledge a plugin load or unload")
    public DynamicPluginConfig setWorkerTimeout(Duration workerTimeout)
    {
        this.workerTimeout = workerTimeout;
        return this;
    }
}
