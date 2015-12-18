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
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.common.io.CountingInputStream;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;

import java.io.IOException;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ThriftRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(ThriftRecordCursor.class);

    private final ThriftColumnHandle[] columnHandles;
    private final short[] fieldToThriftId;

    private final Iterator<String> lines;
    private final long totalBytes;

    private TBase curThriftValue = null;

    // thrift related. should be moved to another class
    private final TDeserializer tDeserializer = new TDeserializer(); // change this to one in EB once 4.11 is released
    private final TypeRef<? extends TBase<?, ?>> tTypeRef;
    private final ThriftToPresto thriftToPresto;

    public ThriftRecordCursor(List<ThriftColumnHandle> columnHandles, String thriftClassName, ByteSource byteSource)
    {
        this.columnHandles = columnHandles.toArray(new ThriftColumnHandle[columnHandles.size()]);

        fieldToThriftId = new short[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            ThriftColumnHandle columnHandle = columnHandles.get(i);
            fieldToThriftId[i] = columnHandle.getThriftFieldId();
        }

        try (CountingInputStream input = new CountingInputStream(byteSource.openStream())) {
            lines = byteSource.asCharSource(UTF_8).readLines().iterator();
            totalBytes = input.getCount();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        tTypeRef = ThriftUtils.getTypeRef(thriftClassName);
        thriftToPresto = new ThriftToPresto(tTypeRef.getRawClass());
    }

    @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.length, "Invalid field index");
        return columnHandles[field].getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        while (true) {
            curThriftValue = null;
            if (!lines.hasNext()) {
                return false;
            }
            String line = lines.next();
            if (line.length() == 0) {
                continue;
            }
            TBase tObj = tTypeRef.safeNewInstance();
            try {
                tDeserializer.deserialize(tObj, Base64.getDecoder().decode(line));
                curThriftValue = tObj;
                return true;
            }
            catch (Exception e) {
                log.warn(e, "Exception while decoding '" + line + "'");
            }
        }
    }

    private Object getFieldValue(int field)
    {
        checkState(curThriftValue != null, "Cursor has not been advanced yet");

        return thriftToPresto.getPrestoValue(
            columnHandles[field].getColumnType(),
            fieldToThriftId[field],
            curThriftValue);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return (Boolean) getFieldValue(field);
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        return (Long) getFieldValue(field);
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return (Double) getFieldValue(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        return (Slice) getFieldValue(field);
    }

    @Override
    public Object getObject(int field)
    {
        return getFieldValue(field);
    }

    @Override
    public boolean isNull(int field)
    {
        return getFieldValue(field) == null;
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }
}
