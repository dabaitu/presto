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

import java.util.List;

public class ThriftColumnMetadata
{
    private final List<ColumnMetadata> columnMetadata;
    private final List<Short> thriftIds;

    public ThriftColumnMetadata(List<ColumnMetadata> columnMetadata, List<Short> thriftIds)
    {
        this.columnMetadata = columnMetadata;
        this.thriftIds = thriftIds;
    }

    public List<ColumnMetadata> getColumnMetadata()
    {
        return columnMetadata;
    }

    public List<Short> getThriftIds()
    {
        return thriftIds;
    }
}
