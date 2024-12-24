package com.stonewu.flink.udf.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class WindowAggregationExample {
    public static void main(String[] args) throws Exception {
        // 创建 TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 注册UDF
        tEnv.executeSql("CREATE TEMPORARY FUNCTION get_first_value AS 'com.stonewu.flink.udf.aggregation.FirstValueFunction';"); 
        tEnv.executeSql("CREATE TEMPORARY FUNCTION get_last_value AS 'com.stonewu.flink.udf.aggregation.LastValueFunction';");

        // 创建源表
        tEnv.executeSql("CREATE TABLE source_table (" +
                "user_id int," +
                "amount DOUBLE," +
                "ts TIMESTAMP(3)," +
                "WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
                ") WITH (" +
                "'connector' = 'datagen'," +
                // 用户ID配置
                "'fields.user_id.kind'='random'," +
                "'fields.user_id.min'='1'," +
                "'fields.user_id.max'='2'," +
                // 金额配置
                "'fields.amount.kind'='random'," +
                "'fields.amount.min'='1.0'," +
                "'fields.amount.max'='1000.0'," +
                // 时间戳配置
                "'fields.ts.max-past'='30 m'," +
                // 控制生成速度
                "'rows-per-second'='1'" +
                ")");

        // 创建结果表（打印到控制台）
        tEnv.executeSql("CREATE TABLE print_table (" +
                "user_id int," +
                "window_start STRING," +
                "window_end STRING," +
                "first_timestamp STRING," +
                "last_timestamp STRING," +
                "first_amount DOUBLE," +
                "last_amount DOUBLE" +
                ") WITH ('connector' = 'print')");

        // 执行窗口聚合查询
        String sql = 
            "INSERT INTO print_table " +
            "SELECT " +
            "    user_id, " +
            "    cast(window_start as string) as window_start, " +
            "    cast(window_end as string) as window_end, " +
            "    get_first_value(cast(ts as string), UNIX_TIMESTAMP(CAST(ts AS STRING))) as first_timestamp, " +
            "    get_last_value(cast(ts as string), UNIX_TIMESTAMP(CAST(ts AS STRING))) as last_timestamp, " +
            "    get_first_value(amount, UNIX_TIMESTAMP(CAST(ts AS STRING))) as first_amount, " +
            "    get_last_value(amount, UNIX_TIMESTAMP(CAST(ts AS STRING))) as last_amount " +
            "FROM TABLE(" +
            "    HOP(TABLE source_table, DESCRIPTOR(ts), " +
            "        INTERVAL '1' MINUTE, " +  // 滑动步长
            "        INTERVAL '10' MINUTE)" +  // 窗口大小
            ") " +
            "GROUP BY user_id, window_start, window_end";

        // 执行查询
        TableResult result = tEnv.executeSql(sql);
        
        // 等待作业完成（实际上是一个无限流作业）
        result.await();
    }
} 