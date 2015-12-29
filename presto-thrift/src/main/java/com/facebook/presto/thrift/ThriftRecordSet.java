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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;

import java.io.File;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ThriftRecordSet
        implements RecordSet
{
    private final List<ThriftColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final String thriftClassName;
    private final ByteSource byteSource;

    public ThriftRecordSet(ThriftSplit split, List<ThriftColumnHandle> columnHandles)
    {
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ThriftColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();

        thriftClassName = split.getThriftClassName();
        byteSource = Files.asByteSource(new File(split.getUri()));
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new ThriftRecordCursor(columnHandles, thriftClassName, byteSource);
    }
}
