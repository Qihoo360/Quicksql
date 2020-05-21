### Original project

https://github.com/Qihoo360/Quicksql


## 已支持clickhouse(clickhouse is supported)

#### 从clickhouse中采集元数据(collecting metadata from Clickhouse)
````
qsql-0.7.0/bin/metadata-extract.sh -p "{ \"jdbcDriver\": \"ru.yandex.clickhouse.ClickHouseDriver\", \"jdbcUrl\": \"jdbc:clickhouse://127.0.0.1:8123/db_test\", \"jdbcUser\": \"default\"}" -d "clickhouse"
````

#### 查询(query)
````
qsql-0.7.0/bin/quicksql.sh -e "select * from db_test.test_click_table LIMIT 10 OFFSET 0" --runner jdbc
````





