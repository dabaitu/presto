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
package com.facebook.presto.thrift;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.twitter.elephantbird.util.ThriftUtils;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.thrift.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ThriftMetadata
        implements ConnectorMetadata
{
    private final String connectorId;
    private final TypeManager typeManager;
    private final ThriftClient thriftClient;

    // cache of class name -> ThriftColumnMetadata
    private LoadingCache<String, ThriftColumnMetadata> columnMetadataCache = CacheBuilder
        .newBuilder()
        .build(CacheLoader.from(this::getColumnMetadataForClass));

    @Inject
    public ThriftMetadata(ThriftConnectorId connectorId, TypeManager typeManager, ThriftClient thriftClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.thriftClient = requireNonNull(thriftClient, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        ThriftPlugin.tmplog("schemaNames 1");
        return listSchemaNames();
    }

    public List<String> listSchemaNames()
    {
        ThriftPlugin.tmplog("schemaNames 2");
        return ImmutableList.copyOf(thriftClient.getSchemaNames());
    }

    @Override
    public ThriftTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        ThriftPlugin.tmplog("getTableHandle");
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        ThriftTable table = thriftClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new ThriftTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        ThriftPlugin.tmplog("getTableLayouts");
        ThriftTableHandle tableHandle = checkType(table, ThriftTableHandle.class, "table");
        ConnectorTableLayout layout = new ConnectorTableLayout(
                new ThriftTableLayoutHandle(tableHandle),
                Optional.empty(),
                TupleDomain.<ColumnHandle>all(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        ThriftPlugin.tmplog("getTableLayout");
        ThriftTableLayoutHandle layout = checkType(handle, ThriftTableLayoutHandle.class, "layout");
        return new ConnectorTableLayout(layout, Optional.empty(), TupleDomain.<ColumnHandle>all(), Optional.empty(), Optional.empty(), ImmutableList.of());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ThriftPlugin.tmplog("getTableMetadata");
        ThriftTableHandle thriftTableHandle = checkType(table, ThriftTableHandle.class, "table");
        checkArgument(thriftTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName tableName = new SchemaTableName(thriftTableHandle.getSchemaName(), thriftTableHandle.getTableName());

        return getTableMetadata(tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ThriftPlugin.tmplog("listTables");
        Set<String> schemaNames;
        if (schemaNameOrNull != null) {
            schemaNames = ImmutableSet.of(schemaNameOrNull);
        }
        else {
            schemaNames = thriftClient.getSchemaNames();
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : thriftClient.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ThriftPlugin.tmplog("getColumnHandles");
        ThriftTableHandle thriftTableHandle = checkType(tableHandle, ThriftTableHandle.class, "tableHandle");
        checkArgument(thriftTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        ThriftTable table = thriftClient.getTable(thriftTableHandle.getSchemaName(), thriftTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(thriftTableHandle.toSchemaTableName());
        }

        ThriftColumnMetadata metadata = columnMetadataCache.getUnchecked(table.getThriftClassName());

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : metadata.getColumnMetadata()) {
            short thriftId = metadata.getThriftIds().get(index);
            columnHandles.put(column.getName(), new ThriftColumnHandle(connectorId, column.getName(), column.getType(), thriftId));
            index++;
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ThriftPlugin.tmplog("listTableColumns");
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }

        ThriftTable table = thriftClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        ThriftColumnMetadata metadata = columnMetadataCache.getUnchecked(table.getThriftClassName());
        return new ConnectorTableMetadata(tableName, metadata.getColumnMetadata());
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ThriftPlugin.tmplog("getColumnMetadata");
        checkType(tableHandle, ThriftTableHandle.class, "tableHandle");
        return checkType(columnHandle, ThriftColumnHandle.class, "columnHandle").getColumnMetadata();
    }

    private ThriftColumnMetadata getColumnMetadataForClass(String className)
    {
        return ThriftToPresto.prestoColumnMetadata(
            ThriftUtils.getTypeRef(className).getRawClass(),
            typeManager
        );
    }
}
