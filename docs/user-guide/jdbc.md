## JDBC Connections

[TOC]

### Overview

AutomaticConnection is one of the methods for querying in Quicksql, and you can get it with `SqlRunner`. It implements the JDBC methods through which you can link almost all the database. Yes, you can connect with Quicksql with JDBC.  

### Getting Started

Add Quicksql jars:`qsql-core-0.6.jar`,`qsql-calcite-elasticsearch-0.6.jar`,`qsql-calcite-analysis-0.6.jar`  into your
dependent libs and you can connect with Quicksql.

Here is the example,

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

