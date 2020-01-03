package com.qihoo.qsql.api;


import com.qihoo.qsql.exec.AbstractPipeline;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

/**
 * SqlRunner is the main class for building and executing qsql task.
 * <p>
 * As the parent class for {@link DynamicSqlRunner}, SqlRunner takes works of initializing and managing environment
 * params mostly.
 * </p>
 */
public abstract class SqlRunner {

    protected Builder environment;

    public SqlRunner(Builder builder) {
        this.environment = builder;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Connection getConnection() throws SQLException {
        return new AutomaticConnection();
    }

    public static Connection getConnection(String schema) throws SQLException {
        return new AutomaticConnection(schema);
    }

    public abstract AbstractPipeline sql(String sql);

    public abstract void stop();

    public static class Builder {

        private RunnerType runner = RunnerType.DEFAULT;
        private Properties runnerProperties = new Properties();

        private Integer resultsNum = 1000;
        private String confPath = "";
        private String appName = "QSQL-" + System.currentTimeMillis();
        private String master = "local[*]";
        private Boolean enableHive = true;

        private Builder() {
        }

        /**
         * Set running engine.
         *
         * @param engine Running engine
         * @return SqlRunner builder, which holds several environment params
         */
        public Builder setTransformRunner(RunnerType engine) {
            switch (engine) {
                case FLINK:
                    this.runner = RunnerType.FLINK;
                    break;
                case SPARK:
                    this.runner = RunnerType.SPARK;
                    break;
                case JDBC:
                    this.runner = RunnerType.JDBC;
                    break;
                default:
                    this.runner = RunnerType.DEFAULT;
            }

            return this;
        }

        public boolean isSpark() {
            return this.runner == RunnerType.SPARK || this.runner == RunnerType.DEFAULT;
        }

        public boolean isFlink() {
            return this.runner == RunnerType.FLINK;
        }

        public boolean isDefaultMode() {
            return this.runner == RunnerType.DEFAULT;
        }

        public boolean isJdbcMode() {
            return this.runner == RunnerType.JDBC;
        }

        public Builder setProperties(Properties properties) {
            this.runnerProperties.putAll(properties);
            return this;
        }

        public String getAppName() {
            return appName;
        }

        public Builder setAppName(String appName) {
            this.appName = appName;
            return this;
        }

        public Boolean getEnableHive() {
            return enableHive;
        }

        public Builder setEnableHive(Boolean enable) {
            this.enableHive = enable;
            return this;
        }

        public String getMaster() {
            return master;
        }

        public Builder setMaster(String master) {
            this.master = master;
            return this;
        }

        public Integer getAcceptedResultsNum() {
            return resultsNum;
        }

        /**
         * Set number of query result which should be showed in console.
         *
         * @param num Number of query result showed in console
         * @return SqlRunner builder, which holds several environment params
         */
        public Builder setAcceptedResultsNum(Integer num) {
            if (num == null || num <= 0) {
                this.resultsNum = 100000;
            } else {
                this.resultsNum = num;
            }
            return this;
        }

        public Properties getRunnerProperties() {
            return runnerProperties;
        }

        public String getSchemaPath() {
            return confPath;
        }

        public Builder setSchemaPath(String schemaPath) {
            this.confPath = schemaPath;
            return this;
        }

        public SqlRunner ok() {
            return new DynamicSqlRunner(this);
        }

        public RunnerType getRunner() {
            return runner;
        }

        public enum RunnerType {
            SPARK, FLINK, JDBC, DEFAULT;

            /**
             * convert to RunnerType from string.
             *
             * @param value runner string value
             * @return enum RunnerType
             */
            public static RunnerType value(String value) {
                value = StringUtils.defaultIfBlank(value,"");
                switch (value.toUpperCase()) {
                    case "SPARK":
                        return SPARK;
                    case "FLINK":
                        return FLINK;
                    case "JDBC":
                        return JDBC;
                    default:
                        return DEFAULT;
                }
            }
        }
    }
}