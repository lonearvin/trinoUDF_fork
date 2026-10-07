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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.operator.table.ExcludeColumnsFunction.ExcludeColumnsFunctionHandle;
import io.trino.operator.table.SequenceFunction.SequenceFunctionHandle;
import io.trino.operator.table.json.JsonTable.JsonTableFunctionHandle;
import io.trino.spi.function.AggregationFunctionMetadata;
import io.trino.spi.function.AggregationImplementation;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionBundle;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.Signature;
import io.trino.spi.function.WindowFunctionSupplier;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.type.TypeManager;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.metadata.OperatorNameUtil.isOperatorName;
import static io.trino.metadata.OperatorNameUtil.mangleOperatorName;
import static io.trino.metadata.OperatorNameUtil.unmangleOperator;
import static io.trino.operator.table.ExcludeColumnsFunction.getExcludeColumnsFunctionProcessorProvider;
import static io.trino.operator.table.SequenceFunction.getSequenceFunctionProcessorProvider;
import static io.trino.operator.table.json.JsonTable.getJsonTableFunctionProcessorProvider;
import static io.trino.spi.function.FunctionKind.AGGREGATE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TypeTemplates.typeVariable;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class GlobalFunctionCatalog
        implements FunctionProvider
{
    public static final String BUILTIN_SCHEMA = "builtin";

    private final Provider<Metadata> metadata;
    private final Provider<TypeManager> typeManager;
    private final Provider<FunctionManager> functionManager;
    private volatile FunctionMap functions = new FunctionMap();
    private volatile Set<FunctionId> dynamicFunctionIds = ImmutableSet.of();
    private final AtomicLong functionVersion = new AtomicLong();

    @Inject
    public GlobalFunctionCatalog(Provider<Metadata> metadata, Provider<TypeManager> typeManager, Provider<FunctionManager> functionManager)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
    }

    public final synchronized void addFunctions(FunctionBundle functionBundle)
    {
        for (FunctionMetadata functionMetadata : functionBundle.getFunctions()) {
            checkArgument(!functions.getFunctionsById().containsKey(functionMetadata.getFunctionId()), "Function already registered: %s", functionMetadata.getFunctionId());

            for (String alias : functionMetadata.getNames()) {
                checkArgument(!alias.contains("|"), "Function name cannot contain '|' character: %s(%s)", alias, functionMetadata.getSignature());
                checkArgument(!alias.contains("@"), "Function name cannot contain '@' character: %s(%s)", alias, functionMetadata.getSignature());
                checkNotSpecializedTypeOperator(alias, functionMetadata.getSignature());

                for (FunctionMetadata existingFunction : this.functions.get(alias)) {
                    checkArgument(!functionMetadata.getSignature().equals(existingFunction.getSignature()), "Function already registered: %s(%s)", alias, functionMetadata.getSignature());
                }
            }
        }
        this.functions = new FunctionMap(this.functions, functionBundle);
        functionVersion.incrementAndGet();
    }

    /**
     * Registers the functions from the bundle, replacing any existing function with the same {@link FunctionId}.
     * This is the entry point used by dynamic plugin loading, where a plugin JAR is reloaded with a new
     * implementation while the engine keeps serving queries.
     * <p>
     * The swap is a single volatile write of an immutable snapshot, so a query that already resolved its
     * functions keeps running against the snapshot it started with.
     */
    public final synchronized void replaceFunctions(FunctionBundle functionBundle)
    {
        Set<FunctionId> replaced = functionBundle.getFunctions().stream()
                .map(FunctionMetadata::getFunctionId)
                .filter(functions.getFunctionsById()::containsKey)
                .collect(toImmutableSet());
        this.functions = new FunctionMap(this.functions, replaced, functionBundle);
        Set<FunctionId> nextDynamicFunctionIds = new java.util.HashSet<>(dynamicFunctionIds);
        nextDynamicFunctionIds.removeAll(replaced);
        functionBundle.getFunctions().stream()
                .map(FunctionMetadata::getFunctionId)
                .forEach(nextDynamicFunctionIds::add);
        dynamicFunctionIds = ImmutableSet.copyOf(nextDynamicFunctionIds);
        functionVersion.incrementAndGet();
    }

    /**
     * Removes the functions with the given ids, used when a dynamically loaded plugin is unloaded.
     * <p>
     * Callers must make sure no in-flight query still references these functions, otherwise the query
     * fails with an unknown function implementation error.
     */
    public final synchronized void removeFunctions(Collection<FunctionId> functionIds)
    {
        this.functions = new FunctionMap(this.functions, ImmutableSet.copyOf(functionIds), null);
        Set<FunctionId> remainingDynamicFunctionIds = new java.util.HashSet<>(dynamicFunctionIds);
        remainingDynamicFunctionIds.removeAll(functionIds);
        dynamicFunctionIds = ImmutableSet.copyOf(remainingDynamicFunctionIds);
        functionVersion.incrementAndGet();
    }

    public boolean isDynamicFunction(FunctionId functionId)
    {
        return dynamicFunctionIds.contains(functionId);
    }

    /**
     * Monotonic version of the function registry, bumped whenever the registered set of functions changes.
     * Caches keyed by function name and argument types include it, so a dynamic plugin reload invalidates
     * them instead of serving the previous implementation.
     */
    public long getFunctionVersion()
    {
        return functionVersion.get();
    }

    /**
     * Type operators are handled automatically by the engine, so custom operator implementations
     * cannot be registered for these.
     */
    private static void checkNotSpecializedTypeOperator(String alias, Signature signature)
    {
        if (!isOperatorName(alias)) {
            return;
        }

        OperatorType operatorType = unmangleOperator(alias);

        // The trick here is the Generic*Operator implementations implement these exact signatures,
        // so we only these exact signatures to be registered.  Since, only a single function with
        // a specific signature can be registered, it prevents others from being registered.
        Signature.Builder expectedSignature = Signature.builder()
                .argumentTypes(Collections.nCopies(operatorType.getArgumentCount(), typeVariable("T")));

        switch (operatorType) {
            case EQUAL, IDENTICAL, INDETERMINATE -> {
                expectedSignature.returnType(BOOLEAN);
                expectedSignature.comparableTypeParameter("T");
            }
            case HASH_CODE, XX_HASH_64 -> {
                expectedSignature.returnType(BIGINT);
                expectedSignature.comparableTypeParameter("T");
            }
            case COMPARISON_UNORDERED_FIRST, COMPARISON_UNORDERED_LAST -> {
                expectedSignature.returnType(INTEGER);
                expectedSignature.orderableTypeParameter("T");
            }
            case LESS_THAN, LESS_THAN_OR_EQUAL -> {
                expectedSignature.returnType(BOOLEAN);
                expectedSignature.orderableTypeParameter("T");
            }
            default -> {
                return;
            }
        }

        checkArgument(signature.equals(expectedSignature.build()), "Can not register %s functionMetadata: %s", operatorType, signature);
    }

    public List<FunctionMetadata> listFunctions()
    {
        return functions.list().stream()
                .filter(function -> !function.isMethod())
                .collect(toImmutableList());
    }

    public Collection<FunctionMetadata> getBuiltInFunctions(String functionName)
    {
        return functions.get(functionName);
    }

    public FunctionMetadata getFunctionMetadata(FunctionId functionId)
    {
        return functions.get(functionId);
    }

    public AggregationFunctionMetadata getAggregationFunctionMetadata(FunctionId functionId)
    {
        return functions.getFunctionBundle(functionId).getAggregationFunctionMetadata(functionId);
    }

    @Override
    public WindowFunctionSupplier getWindowFunctionSupplier(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        return functions.getFunctionBundle(functionId).getWindowFunctionSupplier(functionId, boundSignature, functionDependencies);
    }

    @Override
    public AggregationImplementation getAggregationImplementation(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        return functions.getFunctionBundle(functionId).getAggregationImplementation(functionId, boundSignature, functionDependencies);
    }

    public FunctionDependencyDeclaration getFunctionDependencies(FunctionId functionId, BoundSignature boundSignature)
    {
        return functions.getFunctionBundle(functionId).getFunctionDependencies(functionId, boundSignature);
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(
            FunctionId functionId,
            BoundSignature boundSignature,
            FunctionDependencies functionDependencies,
            InvocationConvention invocationConvention)
    {
        return functions.getFunctionBundle(functionId).getScalarFunctionImplementation(functionId, boundSignature, functionDependencies, invocationConvention);
    }

    @Override
    public TableFunctionProcessorProvider getTableFunctionProcessorProvider(ConnectorTableFunctionHandle functionHandle)
    {
        if (functionHandle instanceof ExcludeColumnsFunctionHandle) {
            return getExcludeColumnsFunctionProcessorProvider();
        }
        if (functionHandle instanceof SequenceFunctionHandle) {
            return getSequenceFunctionProcessorProvider();
        }
        if (functionHandle instanceof JsonTableFunctionHandle) {
            return getJsonTableFunctionProcessorProvider(metadata.get(), typeManager.get(), functionManager.get());
        }

        return null;
    }

    public static boolean isBuiltinFunctionName(CatalogSchemaFunctionName functionName)
    {
        return functionName.catalogName().equals(GlobalSystemConnector.NAME) && functionName.schemaName().equals(BUILTIN_SCHEMA);
    }

    public static CatalogSchemaFunctionName builtinFunctionName(OperatorType operatorType)
    {
        return builtinFunctionName(mangleOperatorName(operatorType));
    }

    public static CatalogSchemaFunctionName builtinFunctionName(String functionName)
    {
        return new CatalogSchemaFunctionName(GlobalSystemConnector.NAME, BUILTIN_SCHEMA, functionName);
    }

    private static class FunctionMap
    {
        private final Map<FunctionId, FunctionBundle> functionBundlesById;
        private final Map<FunctionId, FunctionMetadata> functionsById;
        // function names are currently lower cased
        private final Multimap<String, FunctionMetadata> functionsByLowerCaseName;

        public FunctionMap()
        {
            functionBundlesById = ImmutableMap.of();
            functionsById = ImmutableMap.of();
            functionsByLowerCaseName = ImmutableListMultimap.of();
        }

        public FunctionMap(FunctionMap map, FunctionBundle functionBundle)
        {
            this.functionBundlesById = ImmutableMap.<FunctionId, FunctionBundle>builder()
                    .putAll(map.functionBundlesById)
                    .putAll(functionBundle.getFunctions().stream()
                            .collect(toImmutableMap(FunctionMetadata::getFunctionId, _ -> functionBundle)))
                    .buildOrThrow();

            this.functionsById = ImmutableMap.<FunctionId, FunctionMetadata>builder()
                    .putAll(map.functionsById)
                    .putAll(functionBundle.getFunctions().stream()
                            .collect(toImmutableMap(FunctionMetadata::getFunctionId, Function.identity())))
                    .buildOrThrow();

            ImmutableListMultimap.Builder<String, FunctionMetadata> functionsByName = ImmutableListMultimap.<String, FunctionMetadata>builder()
                    .putAll(map.functionsByLowerCaseName);
            for (FunctionMetadata function : functionBundle.getFunctions()) {
                for (String alias : function.getNames()) {
                    functionsByName.put(alias.toLowerCase(ENGLISH), function);
                }
            }
            this.functionsByLowerCaseName = functionsByName.build();

            // Make sure all functions with the same name are aggregations or none of them are
            for (Entry<String, Collection<FunctionMetadata>> entry : this.functionsByLowerCaseName.asMap().entrySet()) {
                Collection<FunctionMetadata> values = entry.getValue();
                long aggregations = values.stream()
                        .map(FunctionMetadata::getKind)
                        .filter(kind -> kind == AGGREGATE)
                        .count();
                checkState(aggregations == 0 || aggregations == values.size(), "'%s' is both an aggregation and a scalar function", entry.getKey());
            }
        }

        /**
         * Builds a new snapshot by first removing {@code removedIds} and then merging {@code functionBundle}.
         * Passing an empty set and {@code null} yields an identical copy, so unregistering is just a removal.
         */
        public FunctionMap(FunctionMap map, Set<FunctionId> removedIds, FunctionBundle functionBundle)
        {
            Map<FunctionId, FunctionBundle> bundles = new LinkedHashMap<>(map.functionBundlesById);
            Map<FunctionId, FunctionMetadata> metadata = new LinkedHashMap<>(map.functionsById);
            Multimap<String, FunctionMetadata> byName = LinkedHashMultimap.create(map.functionsByLowerCaseName);

            for (FunctionId removedId : removedIds) {
                bundles.remove(removedId);
                FunctionMetadata removed = metadata.remove(removedId);
                if (removed != null) {
                    for (String alias : removed.getNames()) {
                        byName.remove(alias.toLowerCase(ENGLISH), removed);
                    }
                }
            }

            if (functionBundle != null) {
                for (FunctionMetadata function : functionBundle.getFunctions()) {
                    bundles.put(function.getFunctionId(), functionBundle);
                    metadata.put(function.getFunctionId(), function);
                    for (String alias : function.getNames()) {
                        byName.put(alias.toLowerCase(ENGLISH), function);
                    }
                }
            }

            this.functionBundlesById = ImmutableMap.copyOf(bundles);
            this.functionsById = ImmutableMap.copyOf(metadata);
            this.functionsByLowerCaseName = ImmutableListMultimap.copyOf(byName);

            // Make sure all functions with the same name are aggregations or none of them are
            for (Entry<String, Collection<FunctionMetadata>> entry : this.functionsByLowerCaseName.asMap().entrySet()) {
                Collection<FunctionMetadata> values = entry.getValue();
                long aggregations = values.stream()
                        .map(FunctionMetadata::getKind)
                        .filter(kind -> kind == AGGREGATE)
                        .count();
                checkState(aggregations == 0 || aggregations == values.size(), "'%s' is both an aggregation and a scalar function", entry.getKey());
            }
        }

        public List<FunctionMetadata> list()
        {
            return ImmutableList.copyOf(functionsByLowerCaseName.values());
        }

        public Map<FunctionId, FunctionMetadata> getFunctionsById()
        {
            return functionsById;
        }

        public Collection<FunctionMetadata> get(String functionName)
        {
            return functionsByLowerCaseName.get(functionName.toLowerCase(ENGLISH));
        }

        public FunctionMetadata get(FunctionId functionId)
        {
            FunctionMetadata functionMetadata = functionsById.get(functionId);
            checkArgument(functionMetadata != null, "Unknown function implementation: %s", functionId);
            return functionMetadata;
        }

        public FunctionBundle getFunctionBundle(FunctionId functionId)
        {
            FunctionBundle functionBundle = functionBundlesById.get(functionId);
            checkArgument(functionBundle != null, "Unknown function implementation: %s", functionId);
            return functionBundle;
        }
    }
}
