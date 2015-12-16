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

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class ThriftTableLayoutHandle
    implements ConnectorTableLayoutHandle
{
    private final ThriftTableHandle table;

    @JsonCreator
    public ThriftTableLayoutHandle(@JsonProperty("table") ThriftTableHandle table)
    {
        this.table = table;
    }

    @JsonProperty
    public ThriftTableHandle getTable()
    {
        return table;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ThriftTableLayoutHandle that = (ThriftTableLayoutHandle) o;
        return Objects.equal(table, that.table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(table);
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
