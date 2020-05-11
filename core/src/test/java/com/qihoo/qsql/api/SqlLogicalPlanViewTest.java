package com.qihoo.qsql.api;

import org.junit.Test;

public class SqlLogicalPlanViewTest {

    @Test
    public void getLogicalPlanView() {
        String sql = "SELECT dep_id FROM edu_manage.department WHERE dep_id = 1";
        SqlLogicalPlanView sqlLogicalPlanView = new SqlLogicalPlanView();
        System.out.println(sqlLogicalPlanView.getLogicalPlanView(sql));
    }
}