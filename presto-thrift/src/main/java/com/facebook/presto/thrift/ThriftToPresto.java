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
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;
import io.airlift.log.Logger;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TType;

/**
 * This is modelled after ThriftToPig in Elephantbird.
 *
 * Performs two functions :
 *   1) Provides Presto table schema of a Thrift class
 *   2) extracts Presto objects for each column for a give thrift object
 */
public class ThriftToPresto
{
    // XXX Rename this class to something like ThriftType or ThriftColumnType...

    static final Logger log = Logger.get(ThriftPlugin.class);

    // static final String USE_ENUM_ID_CONF_KEY = "elephantbird.pig.thrift.load.enum.as.int";

    // static because it is used in toSchema and toPigTuple which are static
    private static Boolean useEnumId = true;

    private final TStructDescriptor structDesc;
    private final TypeManager typeManager;

    public ThriftToPresto(Class<? extends TBase<?, ?>> tClass, TypeManager typeManager)
    {
        this.structDesc = TStructDescriptor.getInstance(tClass);
        this.typeManager = typeManager;
    }

    public TStructDescriptor getTStructDescriptor()
    {
        return structDesc;
    }

    /**
     * Returns Pig schema for the Thrift struct.
     */
    public static ThriftColumnMetadata prestoColumnMetadata(Class<? extends TBase<?, ?>> tClass,
                                                            TypeManager typeManager)
    {
        return prestoColumnMetadata(TStructDescriptor.getInstance(tClass), typeManager);
    }

    public ThriftColumnMetadata prestoColumnMetadata()
    {
        return prestoColumnMetadata(structDesc, typeManager);
    }

    public static ThriftColumnMetadata prestoColumnMetadata(TStructDescriptor tDesc, TypeManager typeManager)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        ImmutableList.Builder<Short> ids = ImmutableList.builder();
        for (Field field : tDesc.getFields()) {
            // TODO: handle reserved tokens in names (user?)
            columns.add(new ColumnMetadata(field.getName(), prestoType(field, typeManager), false));
            ids.add(field.getFieldId());
        }

        return new ThriftColumnMetadata(columns.build(), ids.build());
    }

    private static Type prestoRowType(TStructDescriptor tDesc, TypeManager typeManager)
    {
        ImmutableList.Builder<TypeSignature> types = ImmutableList.builder();
        ImmutableList.Builder<Object> names = ImmutableList.builder();
        for (Field field : tDesc.getFields()) {
            names.add(field.getName());
            types.add(prestoType(field, typeManager).getTypeSignature());
        }
        return typeManager.getParameterizedType(StandardTypes.ROW, types.build(), names.build());
    }

    private static Type prestoType(Field field, TypeManager typeManager)
    {
        switch (field.getType()) {
            case TType.BOOL:
                return BooleanType.BOOLEAN;

            case TType.BYTE:
            case TType.I16:
            case TType.I32:
            case TType.I64:
                return BigintType.BIGINT;

            case TType.ENUM:
                if (useEnumId) {
                    return BigintType.BIGINT;
                }
                else {
                    return VarcharType.VARCHAR;
                }

            case TType.DOUBLE:
                return DoubleType.DOUBLE;

            case TType.STRING:
                return field.isBuffer() ? VarbinaryType.VARBINARY : VarcharType.VARCHAR;

            case TType.STRUCT:
                return prestoRowType(field.gettStructDescriptor(), typeManager);

            case TType.LIST:
                Type listElemType = prestoType(field.getListElemField(), typeManager);
                return typeManager.getParameterizedType(
                    StandardTypes.ARRAY,
                    ImmutableList.of(listElemType.getTypeSignature()),
                    ImmutableList.of());

            case TType.SET:
                Type setElemType = prestoType(field.getSetElemField(), typeManager);
                return typeManager.getParameterizedType(
                    StandardTypes.ARRAY,
                    ImmutableList.of(setElemType.getTypeSignature()),
                    ImmutableList.of());

            case TType.MAP:
                Type keyType = prestoType(field.getMapKeyField(), typeManager);
                Type valueType = prestoType(field.getMapValueField(), typeManager);
                return typeManager.getParameterizedType(
                    StandardTypes.MAP,
                    ImmutableList.of(keyType.getTypeSignature(), valueType.getTypeSignature()),
                    ImmutableList.of());

            default:
                throw new IllegalArgumentException("Unknown type " + field.getType());
        }
    }
}
