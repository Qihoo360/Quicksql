[English](../../user-guide/jdbc.md)|[中文](./jdbc.md)

## JDBC 连接

[TOC]

### 前言

AutomaticConnection 是使用Quicksql进行查询的方法之一，你可以通过SqlRunner获取它。Quicksql实现了JDBC的连接，对，就是那个可以连接几乎所有主流数据库的JDBC，你同样可以使用它连接Quicksql。

### 开始执行一个SQL

首先，将Quicksql相关jar包放入你的项目依赖中，包括：`qsql-core-0.6.jar`,`qsql-calcite-elasticsearch-0.6.jar`,`qsql-calcite-analysis-0.6.jar`
，然后你就可以开始写代码啦~

下面是示例代码：

```java
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import com.qihoo.qsql.api.SqlRunner;

public class ConnectionTest {

    public static void main(String[] args) {
        try {
            Connection a = SqlRunner.getConnection();
            Statement statement = a.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT 1");
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
}
```

