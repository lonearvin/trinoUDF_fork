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
import io.trino.server.security.ResourceSecurity;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

import static io.trino.server.security.ResourceSecurity.AccessType.INTERNAL_ONLY;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static jakarta.ws.rs.core.Response.Status.FORBIDDEN;
import static java.util.Objects.requireNonNull;

/**
 * Internal-only worker endpoint for loading and unloading dynamically added plugin JARs.
 * The JAR is transferred as bytes and verified against its SHA-256 checksum before loading.
 * <p>
 * Mutating operations additionally require the dynamic plugin feature to be enabled on this node, so a
 * worker that has the feature switched off cannot be used to load code.
 */
@Path("/v1/plugin")
@ResourceSecurity(INTERNAL_ONLY)
public class DynamicPluginResource
{
    private final PluginManager pluginManager;
    private final NodeInstanceId nodeInstanceId;
    private final boolean enabled;
    private final java.nio.file.Path jarDirectory;
    private final long maxJarSize;

    @Inject
    public DynamicPluginResource(PluginManager pluginManager, NodeInstanceId nodeInstanceId, DynamicPluginConfig config)
    {
        this.pluginManager = requireNonNull(pluginManager, "pluginManager is null");
        this.nodeInstanceId = requireNonNull(nodeInstanceId, "nodeInstanceId is null");
        this.enabled = config.isEnabled();
        this.jarDirectory = config.getJarDirectory().map(File::toPath).orElse(PluginArtifact.CACHE_DIRECTORY);
        this.maxJarSize = config.getMaxJarSize().toBytes();
    }

    /**
     * Reports the identity of the serving process together with the plugins it has loaded. The coordinator
     * requires the instance id to match the process it currently observes before trusting the plugin list.
     */
    @GET
    @Produces(TEXT_PLAIN)
    public String listPlugins()
    {
        String plugins = pluginManager.listDynamicPlugins().stream()
                .map(plugin -> plugin.name() + "=" + plugin.checksum())
                .sorted()
                .collect(Collectors.joining("\n"));
        return "instance=" + nodeInstanceId.getId() + "\n" + plugins;
    }

    @GET
    @Path("/{pluginName}")
    @Produces(TEXT_PLAIN)
    public Response getPluginChecksum(@PathParam("pluginName") String pluginName)
    {
        return pluginManager.getDynamicPluginChecksum(pluginName)
                .map(checksum -> Response.ok(checksum).build())
                .orElseGet(() -> Response.status(404).build());
    }

    @POST
    @Path("/{pluginName}")
    @Consumes("application/java-archive")
    public Response addPlugin(@PathParam("pluginName") String pluginName, @QueryParam("checksum") String checksum, InputStream body)
    {
        try {
            if (!enabled) {
                return Response.status(FORBIDDEN).entity("Dynamic plugin loading is disabled on this node").type(TEXT_PLAIN).build();
            }
            if (!pluginName.matches("[a-z0-9][a-z0-9_.-]{0,127}")) {
                return Response.status(BAD_REQUEST).entity("Invalid plugin name").type(TEXT_PLAIN).build();
            }
            if (checksum == null || !checksum.matches("[0-9a-f]{64}")) {
                return Response.status(BAD_REQUEST).entity("Invalid SHA-256 checksum").type(TEXT_PLAIN).build();
            }

            byte[] artifact;
            try {
                // The body is read with a hard cap so an oversized upload cannot be buffered in full before the
                // size check runs. The HTTP server itself has no request body limit, only a header limit.
                artifact = readBounded(body, maxJarSize);
            }
            catch (PayloadTooLargeException _) {
                return Response.status(413).entity("Plugin artifact exceeds the configured size limit").type(TEXT_PLAIN).build();
            }
            if (artifact.length == 0) {
                return Response.status(BAD_REQUEST).entity("Empty plugin artifact").type(TEXT_PLAIN).build();
            }

            String actual = PluginArtifact.checksum(artifact);
            if (!actual.equals(checksum)) {
                return Response.status(BAD_REQUEST)
                        .entity("Checksum mismatch: expected %s but received %s".formatted(checksum, actual))
                        .type(TEXT_PLAIN)
                        .build();
            }
            URL url = PluginArtifact.materialize(jarDirectory, checksum, artifact);
            pluginManager.installPluginFromUrls(pluginName, List.of(url), checksum);
            return Response.ok().build();
        }
        catch (Exception e) {
            return Response.serverError()
                    .entity(e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage())
                    .type(TEXT_PLAIN)
                    .build();
        }
    }

    private static byte[] readBounded(InputStream in, long limit)
            throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        long total = 0;
        int read;
        while ((read = in.read(buffer)) != -1) {
            total += read;
            if (total > limit) {
                throw new PayloadTooLargeException();
            }
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }

    private static final class PayloadTooLargeException
            extends IOException {}

    @DELETE
    @Path("/{pluginName}")
    public Response dropPlugin(@PathParam("pluginName") String pluginName)
    {
        try {
            if (!enabled) {
                return Response.status(FORBIDDEN).entity("Dynamic plugin loading is disabled on this node").type(TEXT_PLAIN).build();
            }
            if (!pluginName.matches("[a-z0-9][a-z0-9_.-]{0,127}")) {
                return Response.status(BAD_REQUEST).entity("Invalid plugin name").type(TEXT_PLAIN).build();
            }
            pluginManager.uninstallPluginIfPresent(pluginName);
            return Response.noContent().build();
        }
        catch (Exception e) {
            return Response.serverError()
                    .entity(e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage())
                    .type(TEXT_PLAIN)
                    .build();
        }
    }
}
