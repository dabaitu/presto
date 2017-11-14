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
package com.facebook.presto.tests.hive;

import com.facebook.presto.jdbc.PrestoArray;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requirement;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.Requires;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.fulfillment.table.MutableTableRequirement;
import com.teradata.tempto.fulfillment.table.MutableTablesState;
import com.teradata.tempto.fulfillment.table.TableDefinition;
import com.teradata.tempto.fulfillment.table.TableHandle;
import com.teradata.tempto.fulfillment.table.TableInstance;
import com.teradata.tempto.fulfillment.table.hive.HiveTableDefinition;
import com.teradata.tempto.query.QueryExecutor;
import com.teradata.tempto.query.QueryResult;
import com.teradata.tempto.query.QueryType;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.tests.TestGroups.HIVE_COERCION;
import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.usingPrestoJdbcDriver;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.usingTeradataJdbcDriver;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.context.ThreadLocalTestContextHolder.testContext;
import static com.teradata.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static com.teradata.tempto.fulfillment.table.TableHandle.tableHandle;
import static com.teradata.tempto.query.QueryExecutor.defaultQueryExecutor;
import static com.teradata.tempto.query.QueryExecutor.query;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static java.lang.String.format;
import static java.sql.JDBCType.ARRAY;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.JAVA_OBJECT;
import static java.sql.JDBCType.LONGNVARCHAR;
import static java.sql.JDBCType.SMALLINT;
import static java.sql.JDBCType.VARBINARY;
import static org.testng.Assert.assertEquals;

public class TestHiveCoercion
        extends ProductTest
{
    private static String tableNameFormat = "%s_hive_coercion";
    private static String dummyTableNameFormat = "%s_dummy";

    public static final HiveTableDefinition HIVE_COERCION_TEXTFILE = tableDefinitionBuilder("TEXTFILE", Optional.empty(), Optional.of("DELIMITED FIELDS TERMINATED BY '|'"))
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_COERCION_PARQUET = parquetTableDefinitionBuilder()
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_COERCION_ORC = tableDefinitionBuilder("ORC", Optional.empty(), Optional.empty())
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_COERCION_RCTEXT = tableDefinitionBuilder("RCFILE", Optional.of("RCTEXT"), Optional.of("SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'"))
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_COERCION_RCBINARY = tableDefinitionBuilder("RCFILE", Optional.of("RCBINARY"), Optional.of("SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe'"))
            .setNoData()
            .build();

    private static HiveTableDefinition.HiveTableDefinitionBuilder tableDefinitionBuilder(String fileFormat, Optional<String> recommendTableName, Optional<String> rowFormat)
    {
        String tableName = format(tableNameFormat, recommendTableName.orElse(fileFormat).toLowerCase(Locale.ENGLISH));
        return HiveTableDefinition.builder(tableName)
                .setCreateTableDDLTemplate("" +
                        "CREATE TABLE %NAME%(" +
                        "    tinyint_to_smallint        TINYINT," +
                        "    tinyint_to_int             TINYINT," +
                        "    tinyint_to_bigint          TINYINT," +
                        "    smallint_to_int            SMALLINT," +
                        "    smallint_to_bigint         SMALLINT," +
                        "    int_to_bigint              INT," +
                        "    bigint_to_varchar          BIGINT," +
                        "    float_to_double            FLOAT," +
                        "    row_to_row                 STRUCT < tinyint_to_smallint : TINYINT, tinyint_to_int : TINYINT, tinyint_to_bigint : TINYINT, smallint_to_int : SMALLINT, smallint_to_bigint : SMALLINT, int_to_bigint : INT, bigint_to_varchar : BIGINT, float_to_double : FLOAT >," +
                        "    list_to_list               ARRAY < STRUCT < tinyint_to_smallint : TINYINT, tinyint_to_int : TINYINT, tinyint_to_bigint : TINYINT, smallint_to_int : SMALLINT, smallint_to_bigint : SMALLINT, int_to_bigint : INT, bigint_to_varchar : BIGINT, float_to_double : FLOAT > >," +
                        "    map_to_map                 MAP < TINYINT, STRUCT < tinyint_to_smallint : TINYINT, tinyint_to_int : TINYINT, tinyint_to_bigint : TINYINT, smallint_to_int : SMALLINT, smallint_to_bigint : SMALLINT, int_to_bigint : INT, bigint_to_varchar : BIGINT, float_to_double : FLOAT > >" +
                        ") " +
                        "PARTITIONED BY (id BIGINT) " +
                        (rowFormat.isPresent() ? "ROW FORMAT " + rowFormat.get() + " " : " ") +
                        "STORED AS " + fileFormat);
    }

    private static HiveTableDefinition.HiveTableDefinitionBuilder parquetTableDefinitionBuilder()
    {
        return HiveTableDefinition.builder("parquet_hive_coercion")
                .setCreateTableDDLTemplate("" +
                        "CREATE TABLE %NAME%(" +
                        "    tinyint_to_smallint        TINYINT," +
                        "    tinyint_to_int             TINYINT," +
                        "    tinyint_to_bigint          TINYINT," +
                        "    smallint_to_int            SMALLINT," +
                        "    smallint_to_bigint         SMALLINT," +
                        "    int_to_bigint              INT," +
                        "    bigint_to_varchar          BIGINT," +
                        "    float_to_double            DOUBLE," +
                        "    row_to_row                 STRUCT < tinyint_to_smallint : TINYINT, tinyint_to_int : TINYINT, tinyint_to_bigint : TINYINT, smallint_to_int : SMALLINT, smallint_to_bigint : SMALLINT, int_to_bigint : INT, bigint_to_varchar : BIGINT, float_to_double : DOUBLE >," +
                        "    list_to_list               ARRAY < STRUCT < tinyint_to_smallint : TINYINT, tinyint_to_int : TINYINT, tinyint_to_bigint : TINYINT, smallint_to_int : SMALLINT, smallint_to_bigint : SMALLINT, int_to_bigint : INT, bigint_to_varchar : BIGINT, float_to_double : DOUBLE > >," +
                        "    map_to_map                 MAP < TINYINT, STRUCT < tinyint_to_smallint : TINYINT, tinyint_to_int : TINYINT, tinyint_to_bigint : TINYINT, smallint_to_int : SMALLINT, smallint_to_bigint : SMALLINT, int_to_bigint : INT, bigint_to_varchar : BIGINT, float_to_double : DOUBLE > >" +
                        ") " +
                        "PARTITIONED BY (id BIGINT) " +
                        "STORED AS PARQUET");
    }

    private static String getCreateDummyTableDDL(HiveTableDefinition tableDefinition)
    {
        String tableName = mutableTableInstanceOf(tableDefinition).getNameInDatabase();
        String floatToDoubleType = tableName.toLowerCase(Locale.ENGLISH).contains("parquet") ? "DOUBLE" : "FLOAT";
        return tableDefinition.getCreateTableDDL(format(dummyTableNameFormat, tableName), Optional.empty())
                    .replace(format("    float_to_double            %s,", floatToDoubleType), format("    float_to_double            %s", floatToDoubleType))
                    .replace(format("    row_to_row                 STRUCT < tinyint_to_smallint : TINYINT, tinyint_to_int : TINYINT, tinyint_to_bigint : TINYINT, smallint_to_int : SMALLINT, smallint_to_bigint : SMALLINT, int_to_bigint : INT, bigint_to_varchar : BIGINT, float_to_double : %s >,", floatToDoubleType), "")
                    .replace(format("    list_to_list               ARRAY < STRUCT < tinyint_to_smallint : TINYINT, tinyint_to_int : TINYINT, tinyint_to_bigint : TINYINT, smallint_to_int : SMALLINT, smallint_to_bigint : SMALLINT, int_to_bigint : INT, bigint_to_varchar : BIGINT, float_to_double : %s > >,", floatToDoubleType), "")
                    .replace(format("    map_to_map                 MAP < TINYINT, STRUCT < tinyint_to_smallint : TINYINT, tinyint_to_int : TINYINT, tinyint_to_bigint : TINYINT, smallint_to_int : SMALLINT, smallint_to_bigint : SMALLINT, int_to_bigint : INT, bigint_to_varchar : BIGINT, float_to_double : %s > >", floatToDoubleType), "");
    }

    public static final class TextRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(HIVE_COERCION_TEXTFILE).withState(CREATED).build();
        }
    }

    public static final class OrcRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(HIVE_COERCION_ORC).withState(CREATED).build();
        }
    }

    public static final class RcTextRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(HIVE_COERCION_RCTEXT).withState(CREATED).build();
        }
    }

    public static final class RcBinaryRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(HIVE_COERCION_RCBINARY).withState(CREATED).build();
        }
    }

    public static final class ParquetRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(HIVE_COERCION_PARQUET).withState(CREATED).build();
        }
    }

    @Requires(TextRequirements.class)
    @Test(groups = {HIVE_COERCION, HIVE_CONNECTOR})
    public void testHiveCoercionTextFile()
            throws SQLException
    {
        doTestHiveCoercion(HIVE_COERCION_TEXTFILE);
    }

    @Requires(OrcRequirements.class)
    @Test(groups = {HIVE_COERCION, HIVE_CONNECTOR})
    public void testHiveCoercionOrc()
            throws SQLException
    {
        doTestHiveCoercion(HIVE_COERCION_ORC);
    }

    @Requires(RcTextRequirements.class)
    @Test(groups = {HIVE_COERCION, HIVE_CONNECTOR})
    public void testHiveCoercionRcText()
            throws SQLException
    {
        doTestHiveCoercion(HIVE_COERCION_RCTEXT);
    }

    @Requires(RcBinaryRequirements.class)
    @Test(groups = {HIVE_COERCION, HIVE_CONNECTOR})
    public void testHiveCoercionRcBinary()
            throws SQLException
    {
        doTestHiveCoercion(HIVE_COERCION_RCBINARY);
    }

    @Requires(ParquetRequirements.class)
    @Test(groups = {HIVE_COERCION, HIVE_CONNECTOR})
    public void testHiveCoercionParquet()
            throws SQLException
    {
        doTestHiveCoercion(HIVE_COERCION_PARQUET);
    }

    private void doTestHiveCoercion(HiveTableDefinition tableDefinition)
            throws SQLException
    {
        String tableName = mutableTableInstanceOf(tableDefinition).getNameInDatabase();
        String dummyTableName = format(dummyTableNameFormat, tableName);
        executeHiveQuery(format("DROP TABLE IF EXISTS %s", dummyTableName));
        executeHiveQuery(getCreateDummyTableDDL(tableDefinition));
        executeHiveQuery(format("INSERT INTO TABLE %s " +
                        "PARTITION (id=1) " +
                        "VALUES" +
                        "(-1, 2, -3, 100, -101, 2323, 12345, 0.5)," +
                        "(1, -2, null, -100, 101, -2323, -12345, -1.5)",
                dummyTableName));
        executeHiveQuery(format("INSERT INTO TABLE %s " +
                        "PARTITION (id=1) " +
                        "SELECT" +
                        "   tinyint_to_smallint," +
                        "   tinyint_to_int," +
                        "   tinyint_to_bigint," +
                        "   smallint_to_int," +
                        "   smallint_to_bigint," +
                        "   int_to_bigint," +
                        "   bigint_to_varchar," +
                        "   float_to_double," +
                        "   named_struct('tinyint_to_smallint', tinyint_to_smallint, 'tinyint_to_int', tinyint_to_int, 'tinyint_to_bigint', tinyint_to_bigint, 'smallint_to_int', smallint_to_int, 'smallint_to_bigint', smallint_to_bigint, 'int_to_bigint', int_to_bigint, 'bigint_to_varchar', bigint_to_varchar, 'float_to_double', float_to_double)," +
                        "   array(named_struct('tinyint_to_smallint', tinyint_to_smallint, 'tinyint_to_int', tinyint_to_int, 'tinyint_to_bigint', tinyint_to_bigint, 'smallint_to_int', smallint_to_int, 'smallint_to_bigint', smallint_to_bigint, 'int_to_bigint', int_to_bigint, 'bigint_to_varchar', bigint_to_varchar, 'float_to_double', float_to_double))," +
                        "   map(tinyint_to_int, named_struct('tinyint_to_smallint', tinyint_to_smallint, 'tinyint_to_int', tinyint_to_int, 'tinyint_to_bigint', tinyint_to_bigint, 'smallint_to_int', smallint_to_int, 'smallint_to_bigint', smallint_to_bigint, 'int_to_bigint', int_to_bigint, 'bigint_to_varchar', bigint_to_varchar, 'float_to_double', float_to_double))" +
                        "FROM %s", tableName, dummyTableName));

        alterTableColumnTypes(tableName);
        assertProperAlteredTableSchema(tableName);

        QueryResult queryResult = query(format("SELECT * FROM %s", tableName));
        assertColumnTypes(queryResult);
        assertThat(queryResult.project(1, 2, 3, 4, 5, 6, 7, 8, 12)).containsOnly(
                row(
                        -1,
                        2,
                        -3L,
                        100,
                        -101L,
                        2323L,
                        "12345",
                        0.5,
                        1),
                row(
                        1,
                        -2,
                        null,
                        -100,
                        101L,
                        -2323L,
                        "-12345",
                        -1.5,
                        1));
        List<Map> rowColumn = ImmutableList.of(
                namedStruct("tinyint_to_smallint", (short) -1, "tinyint_to_int", 2, "tinyint_to_bigint", -3L, "smallint_to_int", 100, "smallint_to_bigint", -101L, "int_to_bigint", 2323L, "bigint_to_varchar", "12345", "float_to_double", 0.5),
                namedStruct("tinyint_to_smallint", (short) 1, "tinyint_to_int", -2, "tinyint_to_bigint", null, "smallint_to_int", -100, "smallint_to_bigint", 101L, "int_to_bigint", -2323L, "bigint_to_varchar", "-12345", "float_to_double", -1.5));
        assertEqualsIgnoreOrder(queryResult.column(9), rowColumn, "row_to_row field is not equal");
        assertEqualsIgnoreOrder(
                queryResult.column(10).stream().map(o -> Arrays.asList((Object[]) ((PrestoArray) o).getArray())).collect(Collectors.toList()),
                rowColumn.stream().map(ImmutableList::of).collect(Collectors.toList()),
                "list_to_list field is not equal");
        assertEqualsIgnoreOrder(queryResult.column(11), rowColumn.stream().map(map -> ImmutableMap.of(map.get("tinyint_to_int"), map)).collect(Collectors.toList()), "map_to_map field is not equal");
    }

    private void assertProperAlteredTableSchema(String tableName)
    {
        assertThat(query("SHOW COLUMNS FROM " + tableName, QueryType.SELECT).project(1, 2)).containsExactly(
                row("tinyint_to_smallint", "smallint"),
                row("tinyint_to_int", "integer"),
                row("tinyint_to_bigint", "bigint"),
                row("smallint_to_int", "integer"),
                row("smallint_to_bigint", "bigint"),
                row("int_to_bigint", "bigint"),
                row("bigint_to_varchar", "varchar"),
                row("float_to_double", "double"),
                row("row_to_row", "row(tinyint_to_smallint smallint, tinyint_to_int integer, tinyint_to_bigint bigint, smallint_to_int integer, smallint_to_bigint bigint, int_to_bigint bigint, bigint_to_varchar varchar, float_to_double double)"),
                row("list_to_list", "array(row(tinyint_to_smallint smallint, tinyint_to_int integer, tinyint_to_bigint bigint, smallint_to_int integer, smallint_to_bigint bigint, int_to_bigint bigint, bigint_to_varchar varchar, float_to_double double))"),
                row("map_to_map", "map(integer, row(tinyint_to_smallint smallint, tinyint_to_int integer, tinyint_to_bigint bigint, smallint_to_int integer, smallint_to_bigint bigint, int_to_bigint bigint, bigint_to_varchar varchar, float_to_double double))"),
                row("id", "bigint"));
    }

    private void assertColumnTypes(QueryResult queryResult)
    {
        Connection connection = defaultQueryExecutor().getConnection();
        if (usingPrestoJdbcDriver(connection)) {
            assertThat(queryResult).hasColumns(
                    SMALLINT,
                    INTEGER,
                    BIGINT,
                    INTEGER,
                    BIGINT,
                    BIGINT,
                    LONGNVARCHAR,
                    DOUBLE,
                    JAVA_OBJECT,
                    ARRAY,
                    JAVA_OBJECT,
                    BIGINT);
        }
        else if (usingTeradataJdbcDriver(connection)) {
            assertThat(queryResult).hasColumns(
                    SMALLINT,
                    INTEGER,
                    BIGINT,
                    INTEGER,
                    BIGINT,
                    BIGINT,
                    VARBINARY,
                    DOUBLE,
                    JAVA_OBJECT,
                    ARRAY,
                    JAVA_OBJECT,
                    BIGINT);
        }
        else {
            throw new IllegalStateException();
        }
    }

    private static void alterTableColumnTypes(String tableName)
    {
        executeHiveQuery(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_smallint tinyint_to_smallint smallint", tableName));
        executeHiveQuery(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_int tinyint_to_int int", tableName));
        executeHiveQuery(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_bigint tinyint_to_bigint bigint", tableName));
        executeHiveQuery(format("ALTER TABLE %s CHANGE COLUMN smallint_to_int smallint_to_int int", tableName));
        executeHiveQuery(format("ALTER TABLE %s CHANGE COLUMN smallint_to_bigint smallint_to_bigint bigint", tableName));
        executeHiveQuery(format("ALTER TABLE %s CHANGE COLUMN int_to_bigint int_to_bigint bigint", tableName));
        executeHiveQuery(format("ALTER TABLE %s CHANGE COLUMN bigint_to_varchar bigint_to_varchar string", tableName));
        executeHiveQuery(format("ALTER TABLE %s CHANGE COLUMN float_to_double float_to_double double", tableName));
        executeHiveQuery(format("ALTER TABLE %s CHANGE COLUMN row_to_row row_to_row struct<tinyint_to_smallint:smallint,tinyint_to_int:int,tinyint_to_bigint:bigint,smallint_to_int:int,smallint_to_bigint:bigint,int_to_bigint:bigint,bigint_to_varchar:string,float_to_double:double>", tableName));
        executeHiveQuery(format("ALTER TABLE %s CHANGE COLUMN list_to_list list_to_list array<struct<tinyint_to_smallint:smallint,tinyint_to_int:int,tinyint_to_bigint:bigint,smallint_to_int:int,smallint_to_bigint:bigint,int_to_bigint:bigint,bigint_to_varchar:string,float_to_double:double>>", tableName));
        executeHiveQuery(format("ALTER TABLE %s CHANGE COLUMN map_to_map map_to_map map<int,struct<tinyint_to_smallint:smallint,tinyint_to_int:int,tinyint_to_bigint:bigint,smallint_to_int:int,smallint_to_bigint:bigint,int_to_bigint:bigint,bigint_to_varchar:string,float_to_double:double>>", tableName));
    }

    private static TableInstance mutableTableInstanceOf(TableDefinition tableDefinition)
    {
        if (tableDefinition.getDatabase().isPresent()) {
            return mutableTableInstanceOf(tableDefinition, tableDefinition.getDatabase().get());
        }
        else {
            return mutableTableInstanceOf(tableHandleInSchema(tableDefinition));
        }
    }

    private static TableInstance mutableTableInstanceOf(TableDefinition tableDefinition, String database)
    {
        return mutableTableInstanceOf(tableHandleInSchema(tableDefinition).inDatabase(database));
    }

    private static TableInstance mutableTableInstanceOf(TableHandle tableHandle)
    {
        return testContext().getDependency(MutableTablesState.class).get(tableHandle);
    }

    private static TableHandle tableHandleInSchema(TableDefinition tableDefinition)
    {
        TableHandle tableHandle = tableHandle(tableDefinition.getName());
        if (tableDefinition.getSchema().isPresent()) {
            tableHandle = tableHandle.inSchema(tableDefinition.getSchema().get());
        }
        return tableHandle;
    }

    private static QueryResult executeHiveQuery(String query)
    {
        return testContext().getDependency(QueryExecutor.class, "hive").executeQuery(query);
    }

    private static Map<Object, Object> namedStruct(Object... objects)
    {
        assertEquals(objects.length % 2, 0, "number of objects must be even");
        Map<Object, Object> struct = new HashMap<>();
        for (int i = 0; i < objects.length; i += 2) {
            struct.put(objects[i], objects[i + 1]);
        }
        return struct;
    }
}
