package com.qihoo.qsql.api;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.org.apache.calcite.rel.TreeNode;
import com.qihoo.qsql.plan.QueryTables;
import com.qihoo.qsql.plan.proc.QueryProcedure;
import com.qihoo.qsql.utils.PropertiesReader;
import com.qihoo.qsql.utils.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * sql logical plan view.
 */
public class SqlLogicalPlanView extends DynamicSqlRunner{

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlLogicalPlanView.class);

    public static final SqlLogicalPlanView VIEW = new SqlLogicalPlanView();

    private SqlLogicalPlanView() {
        super(SqlRunner.builder());
    }

    public static SqlLogicalPlanView getInstance() {
       return VIEW;
    }

    static {
        PropertiesReader.configLogger();
    }

    /**
     * method of logical plan view.
     */
    public String getLogicalPlanView(String sql) {
        sql = sql.replaceAll("explain ","");
        LOGGER.info("The sql being parser: \n" + sql);
        QueryTables tables = SqlUtil.parseTableName(sql);
        super.setTableNames(tables.tableNames);
        environment.setTransformRunner(RunnerType.JDBC);
        QueryProcedure procedure = createQueryPlan(sql);
        return getQueryProcedureLogicalView(procedure);
    }

    public static boolean isPlanView(String sql){
        return sql.toLowerCase().startsWith("explain");
    }

    private String getQueryProcedureLogicalView(QueryProcedure queryProcedure) {
        TreeNode treeNode = queryProcedure.getTreeNode();
        Gson gson =  new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(treeNode);
    }
}
