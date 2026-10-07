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

import com.google.common.hash.Hashing;
import io.trino.spi.TrinoException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Reads plugin JARs from a location and stores them locally under a content-addressed name.
 * <p>
 * Content addressing matters for two reasons: every node can verify it received the exact bytes the
 * coordinator sent, and reloading the same JAR reuses the same file instead of accumulating copies.
 */
public final class PluginArtifact
{
    public static final Path CACHE_DIRECTORY = Path.of(System.getProperty("user.home"), ".trino", "dynamic-plugins");

    private PluginArtifact() {}

    /**
     * Reads the whole JAR from a local path, {@code file:}, or {@code https:} location, enforcing a hard byte limit while reading.
     */
    public static byte[] read(String location, long maxSize)
    {
        try {
            URI uri;
            if (location.startsWith("file:") || location.contains("://")) {
                uri = URI.create(location);
            }
            else {
                uri = Path.of(location).toAbsolutePath().toUri();
            }
            String scheme = uri.getScheme();
            if (scheme == null || !(scheme.equalsIgnoreCase("file") || scheme.equalsIgnoreCase("https"))) {
                throw new IllegalArgumentException("Only file and https JAR locations are supported");
            }
            var connection = uri.toURL().openConnection();
            connection.setConnectTimeout(30_000);
            connection.setReadTimeout(30_000);
            try (InputStream in = connection.getInputStream(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[8192];
                long total = 0;
                int count;
                while ((count = in.read(buffer)) != -1) {
                    total += count;
                    if (total > maxSize) {
                        throw new TrinoException(
                                GENERIC_INTERNAL_ERROR,
                                "Plugin JAR exceeds the configured limit of %s bytes".formatted(maxSize));
                    }
                    out.write(buffer, 0, count);
                }
                return out.toByteArray();
            }
        }
        catch (IOException | IllegalArgumentException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read JAR from %s".formatted(location), e);
        }
    }

    /**
     * SHA-256 of the artifact, used as the identity of a specific JAR version.
     */
    public static String checksum(byte[] artifact)
    {
        return Hashing.sha256().hashBytes(artifact).toString();
    }

    /**
     * Writes the artifact to the local plugin cache and returns a URL that can be handed to a
     * {@code PluginClassLoader}. The file name is the checksum, so the write is idempotent.
     */
    public static URL materialize(String checksum, byte[] artifact)
    {
        return materialize(CACHE_DIRECTORY, checksum, artifact);
    }

    /**
     * Same as {@link #materialize(String, byte[])}, but into an explicit directory.
     */
    public static URL materialize(Path cacheDirectory, String checksum, byte[] artifact)
    {
        if (checksum == null || !checksum.matches("[0-9a-f]{64}") || !checksum(artifact).equals(checksum)) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid SHA-256 checksum for plugin artifact");
        }
        Path target = cacheDirectory.resolve(checksum + ".jar");
        try {
            Files.createDirectories(cacheDirectory);
            boolean validExistingFile = Files.isRegularFile(target) && checksum(Files.readAllBytes(target)).equals(checksum);
            if (!validExistingFile) {
                Path staging = Files.createTempFile(cacheDirectory, checksum, ".tmp");
                Files.write(staging, artifact, WRITE);
                try {
                    // Move into place atomically; another node/thread may have won the race, which is fine.
                    Files.move(staging, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                }
                catch (IOException e) {
                    Files.deleteIfExists(staging);
                    if (!Files.isRegularFile(target) || !checksum(Files.readAllBytes(target)).equals(checksum)) {
                        throw e;
                    }
                }
            }
            return target.toUri().toURL();
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to materialize JAR %s".formatted(checksum), e);
        }
    }
}
