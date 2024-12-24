package com.stonewu.flink.udf.aggregation;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;

@FunctionHint(
    input = {@DataTypeHint("INT"), @DataTypeHint("BIGINT")},
    output = @DataTypeHint("INT")
)
@FunctionHint(
    input = {@DataTypeHint("BIGINT"), @DataTypeHint("BIGINT")},
    output = @DataTypeHint("BIGINT")
)
@FunctionHint(
    input = {@DataTypeHint("TIMESTAMP"), @DataTypeHint("BIGINT")},
    output = @DataTypeHint("TIMESTAMP")
)
@FunctionHint(
    input = {@DataTypeHint("STRING"), @DataTypeHint("BIGINT")},
    output = @DataTypeHint("STRING")
)
@FunctionHint(
    input = {@DataTypeHint("DOUBLE"), @DataTypeHint("BIGINT")},
    output = @DataTypeHint("DOUBLE")
)

public class LastValueFunction<T> extends AggregateFunction<T, LastValueFunction.LastValueAccumulator<T>> {
    
    /**
     * 累加器类，用于存储中间结果
     */
    public static class LastValueAccumulator<T> {
        @DataTypeHint(value = "RAW", bridgedTo = Object.class)
        public T value;
        public boolean hasValue;
        public long timestamp;
    }

    @Override
    public LastValueAccumulator<T> createAccumulator() {
        LastValueAccumulator<T> acc = new LastValueAccumulator<>();
        acc.hasValue = false;
        acc.timestamp = Long.MIN_VALUE;
        return acc;
    }

    public void accumulate(LastValueAccumulator<T> acc, T value, Long timestamp) {
        if (timestamp == null) {
            return;
        }
        if (timestamp < 0) {
            throw new IllegalArgumentException("Timestamp cannot be negative");
        }
        
        if (!acc.hasValue || timestamp > acc.timestamp) {
            acc.value = value;  // value允许为null
            acc.timestamp = timestamp;
            acc.hasValue = true;
        }
    }

    /**
     * 合并两个累加器，用于窗口合并
     */
    public void merge(LastValueAccumulator<T> acc, Iterable<LastValueAccumulator<T>> it) {
        for (LastValueAccumulator<T> otherAcc : it) {
            if (otherAcc.hasValue && otherAcc.timestamp > acc.timestamp) {
                acc.value = otherAcc.value;
                acc.timestamp = otherAcc.timestamp;
                acc.hasValue = true;
            }
        }
    }

    @Override
    public T getValue(LastValueAccumulator<T> acc) {
        if (!acc.hasValue) {
            return null;
        }
        return acc.value;
    }

    /**
     * 处理撤回的数据
     */
    public void retract(LastValueAccumulator<T> acc, T value, Long timestamp) {
        if (timestamp == null) {
            return;
        }
        
        if (acc.hasValue && 
            (value == null ? acc.value == null : value.equals(acc.value)) && 
            timestamp.equals(acc.timestamp)) {
            acc.value = null;
            acc.hasValue = false;
            acc.timestamp = Long.MIN_VALUE;
        }
    }
} 