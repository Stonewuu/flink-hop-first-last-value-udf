# Flink First/Last Value UDF for HOP Window

## 简介

这是一个用于Flink SQL的自定义聚合函数(UDAF)实现,主要用于解决Flink内置的`FIRST_VALUE`和`LAST_VALUE`函数在HOP(滑动窗口)场景下无法使用的问题。

## 功能特点

- 支持在HOP窗口中获取第一个值和最后一个值
- 支持多种数据类型(INT, BIGINT, TIMESTAMP, STRING, DOUBLE)
- 基于事件时间戳排序
- 支持空值处理
- 支持数据撤回(retract)

## 使用方法

1. 注册UDF:
```
CREATE TEMPORARY FUNCTION get_first_value AS 'com.stonewu.flink.udf.aggregation.FirstValueFunction';
CREATE TEMPORARY FUNCTION get_last_value AS 'com.stonewu.flink.udf.aggregation.LastValueFunction';
```

2. 在HOP窗口中使用:
-- 注意，第二个参数是时间戳，需要转换为BIGINT类型
```
SELECT
user_id,
get_first_value(amount, UNIX_TIMESTAMP(CAST(ts AS STRING))) as first_amount,
get_last_value(amount, UNIX_TIMESTAMP(CAST(ts AS STRING))) as last_amount
FROM TABLE(
HOP(TABLE source_table, DESCRIPTOR(ts),
INTERVAL '1' MINUTE, -- 滑动步长
INTERVAL '10' MINUTE) -- 窗口大小
```

## 示例代码

完整的示例代码可以参考:
[WindowAggregationExample.java](src/test/java/com/stonewu/flink/udf/sql/WindowAggregationExample.java)

## 为什么需要这个UDF?

Flink SQL内置的`FIRST_VALUE`和`LAST_VALUE`函数在以下场景中存在限制:

1. 不支持在HOP窗口中使用
2. 不支持基于指定字段(如时间戳)排序
3. 不支持处理撤回流

本UDF通过自定义实现解决了这些限制,使得在HOP窗口中也能方便地获取第一个和最后一个值。

## 构建

项目使用Maven构建:
```
mvn clean package
```

## 依赖要求

- Java 8+
- Apache Flink 1.17+

## 许可证

Apache License 2.0