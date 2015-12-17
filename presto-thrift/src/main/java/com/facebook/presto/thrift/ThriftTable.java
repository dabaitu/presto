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

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.util.ThriftUtils;
import org.apache.thrift.TBase;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class ThriftTable
{
    private final String name;
    private final List<ThriftColumn> columns; // XXX Remove
    private final List<ColumnMetadata> columnsMetadata; // REMOVE
    private final List<URI> sources;
    private final String thriftClassName; // fetch from
    private final boolean datehourPartitioned;
    private final Class<? extends TBase<?, ?>> tClass;
    private final TStructDescriptor tStructDescriptor;

    @JsonCreator
    public ThriftTable(
            @JsonProperty("name") String name,
            @JsonProperty("columns") List<ThriftColumn> columns,
            @JsonProperty("sources") List<URI> sources,
            @JsonProperty("thrift_class_name") String thriftClassName,
            @JsonProperty("datehour_partitioned") boolean datehourPartitioned
            )
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = requireNonNull(name, "name is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));
        this.thriftClassName = thriftClassName; // requireNonNull(thriftClassName, "thrift class name is null");
        this.datehourPartitioned = datehourPartitioned;
        this.tClass = ThriftUtils.getTypeRef(thriftClassName).getRawClass();
        this.tStructDescriptor = TStructDescriptor.getInstance(tClass);

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (ThriftColumn column : this.columns) {
            columnsMetadata.add(new ColumnMetadata(column.getName(), column.getType(), false));
        }
        this.columnsMetadata = columnsMetadata.build();
        ThriftPlugin.tmplog("Table: %s : columns : %s : sources %s",
            name, Arrays.toString(columns.toArray()), Arrays.toString(sources.toArray()));
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<ThriftColumn> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public List<URI> getSources()
    {
        return sources;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }
}
