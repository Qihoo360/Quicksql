package com.qihoo.qsql.codegen.flink;


import com.qihoo.qsql.codegen.QueryGenerator;
import com.qihoo.qsql.codegen.ClassBodyComposer;

/**
 * Code generator, used when {@link com.qihoo.qsql.exec.flink.FlinkPipeline} is chosen and source data of query is in
 * MySql at the same time.
 */
public class FlinkMySqlGenerator extends QueryGenerator {

    private static final String CODE_FUNTION_BODY = "";

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
            "import java.util.List"
        };
        composer.handleComposition(ClassBodyComposer.CodeCategory.IMPORT, imports);
    }

    @Override
    protected void prepareQuery() {
        composer.handleComposition(ClassBodyComposer.CodeCategory.METHOD,
            declareGetDataTypeMethod());
        composer
            .handleComposition(ClassBodyComposer.CodeCategory.METHOD, declareQueryMethod());
        composer.handleComposition(ClassBodyComposer.CodeCategory.INNER_CLASS,
            declareDataSetWrapperClass());
    }

    @Override
    protected void executeQuery() {
        Invoker config = Invoker.registerMethod("query");

        String invokeWrap = config.invoke(convertProperties("jdbcUrl", "jdbcUser", "jdbcPassword"));
        String wrapper = with("wrapper", "tmp");
        String invokedStatement =
            with("DataSetListWrapper", "tmp") + " " + wrapper + " = " + invokeWrap + ";";
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, invokedStatement);

        String invoked =
            "DataSet tmp = env.fromCollection(" + wrapper + ".rows, " + wrapper + ""
                + ".rowTypeInfo);";
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, invoked);

        String created = "tEnv.registerDataSet(\"" + tableName + "\", tmp);";
        composer.handleComposition(ClassBodyComposer.CodeCategory.SENTENCE, created);
    }

    @Override
    public void saveToTempTable() {

    }

    private String declareGetDataTypeMethod() {
        return " private String[] " + with("getDataTypes", "tmp")
            + "(ResultSet resultSet) throws SqlException { \n"
            + "    int n = resultSet.getMetaData().getColumnCount(); \n"
            + "    String[] array = new String[n]; \n"
            + "    for (int index = 1; index <= n; index++) { \n"
            + "        String labelName = resultSet.getMetaData().getColumnLabel(index); \n"
            + "        array[index - 1] = labelName; \n"
            + "    } \n"
            + "    return array; \n"
            + " } \n";
    }

    private String declareQueryMethod() {
        return " private " + with("DataSetListWrapper", "tmp") + " " + with("query", "tmp")
            + "(String url, String user, String password) { \n"
            + "     try { \n"
            + "         Class.forName(\"com.mysql.jdbc.Driver\"); \n"
            + "     } catch (ClassNotFoundException e) { \n"
            + "         e.printStackTrace(); \n"
            + "         throw new RuntimeException(e.getMessage()); \n"
            + "     } \n"
            + "  \n"
            + "    Connection conn = null; \n"
            + "     Statement stmt = null; \n"
            + "     ResultSet resultSet = null; \n"
            + "     List<Row> rows = new ArrayList<>(); \n"
            + "     String[] columns; \n"
            + "     try { \n"
            + "         conn = DriverManager.getConnection(url, user, password); \n"
            + "         String sql = \"" + query.toLowerCase() + "\"; \n"
            + "         stmt = conn.prepareStatement(sql); \n"
            + "         resultSet = stmt.executeQuery(sql); \n"
            + "         columns = " + with("getDataTypes", "tmp") + "(resultSet); \n"
            + "         int n = resultSet.getMetaData().getColumnCount(); \n"
            + "         while (resultSet.next()) { \n"
            + "             Row row = new Row(n); \n"
            + "             for (int index = 1; index <= n; index++) { \n"
            + "                 Object obj = resultSet.getObject(index); \n"
            + "                 row.setField(index - 1, obj); \n"
            + "             } \n"
            + "             rows.add(row); \n"
            + "         } \n"
            + "     } catch (SqlException e) { \n"
            + "         throw new RuntimeException(e.getMessage()); \n"
            + "     } finally { \n"
            + "         try { \n"
            + "             if (resultSet != null) { \nresultSet.close(); \n} \n"
            + "             if (stmt != null) { \nstmt.close(); \n} \n"
            + "             if (conn != null) { \nconn.close(); \n} \n"
            + "         } catch (SqlException e) { \n"
            + "             e.printStackTrace(); \n"
            + "         } \n"
            + "     } \n"
            + "  \n"
            + "     TypeInformation<?>[] types = { \n"
            + "             BasicTypeInfo.LONG_TYPE_INFO, \n"
            + "             BasicTypeInfo.STRING_TYPE_INFO, \n"
            + "             BasicTypeInfo.STRING_TYPE_INFO, \n"
            + "             BasicTypeInfo.STRING_TYPE_INFO, \n"
            + "             BasicTypeInfo.STRING_TYPE_INFO \n"
            + "     }; \n"
            + "  \n"
            + "     String[] fieldNames = columns; \n"
            + "     RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames); \n"
            + "  \n"
            + "     return new " + with("DataSetListWrapper", "tmp") + "(rows, rowTypeInfo); \n"
            + " }  \n";
    }

    private String declareDataSetWrapperClass() {
        return "private class " + with("DataSetListWrapper", "tmp") + " { \n"
            + "    public List<Row> rows; \n"
            + "    public RowTypeInfo rowTypeInfo; \n"
            + " \n"
            + "    public " + with("DataSetListWrapper", "tmp")
            + "(List<Row> rows, RowTypeInfo rowTypeInfo) { \n"
            + "        this.rows = rows; \n"
            + "        this.rowTypeInfo = rowTypeInfo; \n"
            + "    } \n"
            + "} \n";
    }

}

