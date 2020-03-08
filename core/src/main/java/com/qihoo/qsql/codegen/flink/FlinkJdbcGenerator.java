package com.qihoo.qsql.codegen.flink;


import com.qihoo.qsql.codegen.QueryGenerator;
import com.qihoo.qsql.codegen.ClassBodyComposer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

/**
 * Code generator, used when {@link com.qihoo.qsql.exec.flink.FlinkPipeline} is chosen and source data of query is in
 * MySql at the same time.
 */
public class FlinkJdbcGenerator extends QueryGenerator {


    @Override
    protected void importDependency() {
        String[] imports = {
            "import org.apache.flink.api.common.typeinfo.BasicTypeInfo",
            "import org.apache.flink.api.common.typeinfo.TypeInformation",
            "import org.apache.flink.api.java.DataSet",
            "import org.apache.flink.api.java.ExecutionEnvironment",
            "import org.apache.flink.api.java.typeutils.RowTypeInfo",
            "import org.apache.flink.table.api.Table",
            "import org.apache.flink.table.api.TableEnvironment",
            "import org.apache.flink.table.api.java.BatchTableEnvironment",
            "import org.apache.flink.types.Row",
            "import java.sql.*",
            "import java.util.ArrayList",
            "import java.util.List",
            "import com.qihoo.qsql.codegen.flink.FlinkJdbcGenerator",
            "import com.qihoo.qsql.codegen.flink.FlinkJdbcGenerator.DataSetListWrapper"
        };
        composer.handleComposition(ClassBodyComposer.CodeCategory.IMPORT, imports);
    }

    @Override
    protected void prepareQuery() {
        //no action
    }

    @Override
    protected void executeQuery() {
        Invoker config = Invoker.registerMethod("FlinkJdbcGenerator.query");

        String invokeWrap = config.invoke(insertArray(convertProperties("jdbcUrl", "jdbcUser", "jdbcPassword",
            "jdbcDriver"), query));
        String wrapper = with("wrapper", "tmp");
        String invokedStatement = "DataSetListWrapper" + " " + wrapper + " = " + invokeWrap + ";";
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, invokedStatement);

        String invoked =
            "DataSet tmpData = env.fromCollection(" + wrapper + ".rows, " + wrapper + ""
                + ".rowTypeInfo);";
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, invoked);

        String created = "tableEnv.registerDataSet(\"" + tableName + "\", tmpData);";
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, created);
    }

    @Override
    public void saveToTempTable() {

    }


    /**
     * .
     */
    public static TypeInformation<?> getTypeInformation(String type) {
        switch (type) {
            case "INT":
            case "TINYINT":
            case "SMALLINT":
            case "MEDIUMINT":
            case "BOOLEAN":
            case "INTEGER":
                return BasicTypeInfo.INT_TYPE_INFO;
            case "BIGINT":
            case "INT UNSIGNED":
                return BasicTypeInfo.LONG_TYPE_INFO;
            case "VARCHAR":
            case "TEXT":
            case "TIMESTAMP":
            case "DATETIME":
            case "LONGTEXT":
            case "VARCHAR2":
            case "STRING":
            case "CHAR":
                return BasicTypeInfo.STRING_TYPE_INFO;
            case "DOUBLE":
                return BasicTypeInfo.DOUBLE_TYPE_INFO;
            case "FLOAT":
                return BasicTypeInfo.FLOAT_TYPE_INFO;
            case "DATE":
            case "YEAR":
                return SqlTimeTypeInfo.DATE;
            case "BIGDECIMAL":
            case "DECIMAL":
                return BasicTypeInfo.BIG_DEC_TYPE_INFO;
            case "BIT":
                return BasicTypeInfo.BOOLEAN_TYPE_INFO;
            case "BLOB":
            case "LONGBLOB":
                return BasicTypeInfo.BYTE_TYPE_INFO;
            default:
                return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }

    /**
     * .
     */
    public static String[] getDataTypes(ResultSet resultSet) throws SQLException {
        int count = resultSet.getMetaData().getColumnCount();
        String[] array = new String[count];
        for (int index = 1; index <= count; index++) {
            String labelName = resultSet.getMetaData().getColumnLabel(index);
            array[index - 1] = labelName;
        }
        return array;
    }

    /**
     * .
     */
    public static DataSetListWrapper query(String url, String user, String passWord, String jdbcDriver, String sql) {
        try {
            Class.forName(jdbcDriver);
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex.getMessage());
        }
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet resultSet = null;
        List<Row> rows = new ArrayList<>();
        String[] columns;
        List<TypeInformation<?>> typeInformations = new ArrayList<>();
        int rowCount;
        try {
            conn = DriverManager.getConnection(url, user, passWord);
            stmt = conn.prepareStatement(sql);
            resultSet = stmt.executeQuery();
            ResultSetMetaData rsmd = resultSet.getMetaData();
            columns = getDataTypes(resultSet);
            rowCount = resultSet.getMetaData().getColumnCount();
            for (int index = 1; index <= rowCount; index++) {
                typeInformations.add(getTypeInformation(rsmd.getColumnTypeName(index).toUpperCase()));
            }
            while (resultSet.next()) {
                Row row = new Row(rowCount);
                for (int index = 1; index <= rowCount; index++) {
                    Object obj = resultSet.getObject(index);
                    row.setField(index - 1, obj);
                }
                rows.add(row);
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex.getMessage());
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
        TypeInformation<?>[] types = new TypeInformation<?>[rowCount];
        String[] fieldNames = columns;
        RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInformations.toArray(types), fieldNames);

        return new DataSetListWrapper(rows, rowTypeInfo);
    }

    /**
     * .
     */
    public static class DataSetListWrapper {

        public List<Row> rows;
        public RowTypeInfo rowTypeInfo;

        public DataSetListWrapper(List<Row> rows, RowTypeInfo rowTypeInfo) {
            this.rows = rows;
            this.rowTypeInfo = rowTypeInfo;
        }
    }


    private String[] insertArray(String[] arr, String sql) {
        int size = arr.length;
        String[] tmp = new String[size + 1];
        for (int i = 0; i < size; i++) {
            tmp[i] = arr[i];
        }
        tmp[size] = sql;
        return tmp;
    }
}


