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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.hive.HiveColumnHandle;
import parquet.column.Encoding;
import parquet.io.ParquetDecodingException;
import parquet.schema.MessageType;
import parquet.schema.Type;

public final class ParquetTypeUtils
{
    private ParquetTypeUtils()
    {
    }

    public static parquet.schema.Type getParquetType(HiveColumnHandle column, MessageType messageType, boolean useParquetColumnNames)
    {
        if (useParquetColumnNames) {
            return findParquetTypeByName(column, messageType);
        }

        if (column.getHiveColumnIndex() < messageType.getFieldCount()) {
            return messageType.getType(column.getHiveColumnIndex());
        }
        return null;
    }

    /**
     * Find the column type by name using returning the first match with the following logic:
     * <ul>
     * <li>direct match</li>
     * <li>case-insensitive match</li>
     * <li>if the name ends with _, remove it and direct match</li>
     * <li>if the name ends with _, remove it and case-insensitive match</li>
     * </ul>
     */
    private static parquet.schema.Type findParquetTypeByName(HiveColumnHandle column, MessageType messageType)
    {
        String name = column.getName();
        Type type = getParquetTypeByName(name, messageType);

        // when a parquet field is a hive keyword we append an _ to it in hive. When doing
        // a name-based lookup, we need to strip it off again if we didn't get a direct match.
        if (type == null && name.endsWith("_")) {
            type = getParquetTypeByName(name.substring(0, name.length() - 1), messageType);
        }
        return type;
    }

    public static ParquetEncoding getParquetEncoding(Encoding encoding)
    {
        switch (encoding) {
            case PLAIN:
                return ParquetEncoding.PLAIN;
            case RLE:
                return ParquetEncoding.RLE;
            case BIT_PACKED:
                return ParquetEncoding.BIT_PACKED;
            case PLAIN_DICTIONARY:
                return ParquetEncoding.PLAIN_DICTIONARY;
            case DELTA_BINARY_PACKED:
                return ParquetEncoding.DELTA_BINARY_PACKED;
            case DELTA_LENGTH_BYTE_ARRAY:
                return ParquetEncoding.DELTA_LENGTH_BYTE_ARRAY;
            case DELTA_BYTE_ARRAY:
                return ParquetEncoding.DELTA_BYTE_ARRAY;
            case RLE_DICTIONARY:
                return ParquetEncoding.RLE_DICTIONARY;
            default:
                throw new ParquetDecodingException("Unsupported Parquet encoding: " + encoding);
        }
    }

    private static parquet.schema.Type getParquetTypeByName(String columnName, MessageType messageType)
    {
        if (messageType.containsField(columnName)) {
            return messageType.getType(columnName);
        }
        // parquet is case-sensitive, but hive is not. all hive columns get converted to lowercase
        // check for direct match above but if no match found, try case-insensitive match
        for (Type type : messageType.getFields()) {
            if (type.getName().equalsIgnoreCase(columnName)) {
                return type;
            }
        }

        return null;
    }
}
