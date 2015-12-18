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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
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
import com.google.common.collect.ImmutableMap;
import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TType;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

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
    private static Boolean useEnumId = false;

    private final TStructDescriptor structDesc;
    private final Map<Short, Field> tIdToField;

    public ThriftToPresto(Class<? extends TBase<?, ?>> tClass)
    {
        this.structDesc = TStructDescriptor.getInstance(tClass);

        ImmutableMap.Builder<Short, Field> mapBuilder = ImmutableMap.builder();
        for (Field f : structDesc.getFields()) {
            mapBuilder.put(f.getFieldId(), f);
        }
        tIdToField = mapBuilder.build();
    }

    public TStructDescriptor getTStructDescriptor()
    {
        return structDesc;
    }

    public Object getPrestoValue(Type prestoType, short thriftId, TBase tObj)
    {
        Field f = tIdToField.get(thriftId);
        requireNonNull(f, thriftId + " not found in thrift class " + structDesc.getThriftClass());
        Object tValue = tObj.getFieldValue(f.getFieldIdEnum());

        return getPrestoValue(prestoType, f, tValue);
    }

    public static Object getPrestoValue(Type prestoType, Field field, Object tValue)
    {
       switch (field.getType()) {
           case TType.BOOL:
               return tValue;
           case TType.BYTE:
               return ((Byte) tValue).longValue();
           case TType.I16:
               return ((Short) tValue).longValue();
           case TType.I32:
               return ((Integer) tValue).longValue();
           case TType.I64:
               return tValue;

           case TType.ENUM:
               if (useEnumId) {
                   return (long) field.getEnumValueOf(tValue.toString()).getValue();
               }
               else {
                  return Slices.utf8Slice(tValue.toString());
               }

           case TType.DOUBLE:
               return tValue;

           case TType.STRING:
               if (tValue instanceof String) {
                   return Slices.utf8Slice(tValue.toString());
               }
               else if (tValue instanceof byte[]) {
                   return Slices.wrappedBuffer((byte[]) tValue);
               }
               else if (tValue instanceof ByteBuffer) {
                   return Slices.wrappedBuffer((ByteBuffer) tValue);
               }
               else {
                   return null;
               }

           case TType.STRUCT:
           case TType.LIST:
           case TType.SET:
           case TType.MAP:
               return getBlockObject(prestoType, field, tValue);

           default:
               throw new IllegalArgumentException("Unknown type " + field.getType());
       }
    }

    public static Block getBlockObject(Type type, Field field, Object tValue)
    {
        if (tValue == null) {
            return null;
        }

        return serializeObject(type, null, field, tValue);
    }

    public static Block serializeObject(Type prestoType, BlockBuilder builder, Field field, Object tValue)
    {
        switch (field.getType()) {
            case TType.STRUCT:
                return serializeStruct(prestoType, builder, field, tValue);
            case TType.LIST:
            case TType.SET:
                return serializeList(prestoType, builder, field, tValue);
            case TType.MAP:
                return serializeMap(prestoType, builder, field, tValue);
            default:
                serializePrimitive(prestoType, builder, field, tValue);
                return null;
        }
    }

    private static void serializePrimitive(Type prestoType, BlockBuilder builder, Field field, Object tValue)
    {
        requireNonNull(builder, "parent builder is null");

        if (tValue == null) {
            builder.appendNull();
            return;
        }

        switch (field.getType()) {
            case TType.BOOL:
                BooleanType.BOOLEAN.writeBoolean(builder, (Boolean) tValue);
                return;
            case TType.BYTE:
                BigintType.BIGINT.writeLong(builder, (Byte) tValue);
                return;
            case TType.I16:
                BigintType.BIGINT.writeLong(builder, (Short) tValue);
                return;
            case TType.I32:
                BigintType.BIGINT.writeLong(builder, (Integer) tValue);
                return;
            case TType.I64:
                BigintType.BIGINT.writeLong(builder, (Long) tValue);
                return;
            case TType.ENUM:
                if (useEnumId) {
                    BigintType.BIGINT.writeLong(builder, field.getEnumValueOf(tValue.toString()).getValue());
                }
                else {
                    VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(tValue.toString()));
                }
                return;
            case TType.DOUBLE:
                DoubleType.DOUBLE.writeDouble(builder, (Double) tValue);
                return;
            case TType.STRING:
                if (tValue instanceof String) {
                    VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(tValue.toString()));
                }
                else if (tValue instanceof byte[]) {
                    VarbinaryType.VARBINARY.writeSlice(builder, Slices.wrappedBuffer((byte[]) tValue));
                }
                else if (tValue instanceof ByteBuffer) {
                    VarbinaryType.VARBINARY.writeSlice(builder, Slices.wrappedBuffer((ByteBuffer) tValue));
                }
                return;
            default:
                throw new IllegalArgumentException("Not a primitive type " + field.getType());
        }
    }

    private static Block serializeList(Type prestoType, BlockBuilder builder, Field field, Object tValue)
    {
        if (tValue == null) {
            requireNonNull(builder, "parent builder is null").appendNull();
            return null;
        }

        // tValue could be a List or a Set
        Collection<Object> coll = (Collection<Object>) tValue;

        List<Type> typeParameters = prestoType.getTypeParameters();
        checkArgument(typeParameters.size() == 1, "list must have exactly 1 type parameter");
        Type elementType = typeParameters.get(0);
        Field elementField = field.getType() == TType.SET ? field.getSetElemField() : field.getListElemField();
        BlockBuilder currentBuilder;
        if (builder != null) {
            currentBuilder = builder.beginBlockEntry();
        }
        else {
            currentBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), coll.size());
        }

        for (Object element : coll) {
            serializeObject(elementType, currentBuilder, elementField, element);
        }

        if (builder != null) {
            builder.closeEntry();
            return null;
        }
        else {
            Block resultBlock = currentBuilder.build();
            return resultBlock;
        }
    }

    private static Block serializeMap(Type prestoType, BlockBuilder builder, Field field, Object tValue)
    {
        if (tValue == null) {
            requireNonNull(builder, "parent builder is null").appendNull();
            return null;
        }

        Map<?, ?> map = (Map<?, ?>) tValue;

        List<Type> typeParameters = prestoType.getTypeParameters();
        checkArgument(typeParameters.size() == 2, "map must have exactly 2 type parameter");
        Type keyType = typeParameters.get(0);
        Type valueType = typeParameters.get(1);
        Field keyField = field.getMapKeyField();
        Field valueField = field.getMapValueField();
        BlockBuilder currentBuilder;
        if (builder != null) {
            currentBuilder = builder.beginBlockEntry();
        }
        else {
            currentBuilder = new InterleavedBlockBuilder(typeParameters, new BlockBuilderStatus(), map.size());
        }

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            // Hive skips map entries with null keys
            if (entry.getKey() != null) {
                serializeObject(keyType, currentBuilder, keyField, entry.getKey());
                serializeObject(valueType, currentBuilder, valueField, entry.getValue());
            }
        }

        if (builder != null) {
            builder.closeEntry();
            return null;
        }
        else {
            Block resultBlock = currentBuilder.build();
            return resultBlock;
        }
    }

    private static Block serializeStruct(Type prestoType, BlockBuilder builder, Field field, Object tValue)
    {
        if (tValue == null) {
            requireNonNull(builder, "parent builder is null").appendNull();
            return null;
        }

        List<Type> typeParameters = prestoType.getTypeParameters();
        List<Field> structFields = field.gettStructDescriptor().getFields();
        BlockBuilder currentBuilder;
        if (builder != null) {
            currentBuilder = builder.beginBlockEntry();
        }
        else {
            currentBuilder = new InterleavedBlockBuilder(typeParameters, new BlockBuilderStatus(), typeParameters.size());
        }

        TBase tBase = (TBase) tValue;

        for (int i = 0; i < typeParameters.size(); i++) {
            Field f = structFields.get(i);
            serializeObject(typeParameters.get(i), currentBuilder, f, tBase.getFieldValue(f.getFieldIdEnum()));

        }

        if (builder != null) {
            builder.closeEntry();
            return null;
        }
        else {
            Block resultBlock = currentBuilder.build();
            return resultBlock;
        }
    }

    /**
     * Returns Pig schema for the Thrift struct.
     */
    public static ThriftColumnMetadata prestoColumnMetadata(Class<? extends TBase<?, ?>> tClass,
                                                            TypeManager typeManager)
    {
        return prestoColumnMetadata(TStructDescriptor.getInstance(tClass), typeManager);
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
