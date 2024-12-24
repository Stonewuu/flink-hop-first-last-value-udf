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

public class FirstValueFunction<T> extends AggregateFunction<T, FirstValueFunction.FirstValueAccumulator<T>> {
    
    /**
     * 累加器类，用于存储中间结果
     */
    public static class FirstValueAccumulator<T> {
        @DataTypeHint(value = "RAW", bridgedTo = Object.class)
        public T value;
        public boolean hasValue;
        public long timestamp;
    }

    @Override
    public FirstValueAccumulator<T> createAccumulator() {
        FirstValueAccumulator<T> acc = new FirstValueAccumulator<>();
        acc.hasValue = false;
        acc.timestamp = Long.MAX_VALUE;
        return acc;
    }

    public void accumulate(FirstValueAccumulator<T> acc, T value, Long timestamp) {
        if (timestamp == null) {
            return;
        }
        
        if (!acc.hasValue || timestamp < acc.timestamp) {
            acc.value = value;  // value允许为null
            acc.timestamp = timestamp;
            acc.hasValue = true;
        }
    }

    /**
     * 合并两个累加器，用于窗口合并
     */
    public void merge(FirstValueAccumulator<T> acc, Iterable<FirstValueAccumulator<T>> it) {
        for (FirstValueAccumulator<T> otherAcc : it) {
            if (otherAcc.hasValue && otherAcc.timestamp < acc.timestamp) {
                acc.value = otherAcc.value;
                acc.timestamp = otherAcc.timestamp;
                acc.hasValue = true;
            }
        }
    }

    @Override
    public T getValue(FirstValueAccumulator<T> acc) {
        if (!acc.hasValue) {
            return null;
        }
        return acc.value;
    }

    /**
     * 处理撤回的数据
     */
    public void retract(FirstValueAccumulator<T> acc, T value, Long timestamp) {
        if (timestamp == null) {
            return;
        }
        
        if (acc.hasValue && 
            (value == null ? acc.value == null : value.equals(acc.value)) && 
            timestamp.equals(acc.timestamp)) {
            acc.value = null;
            acc.hasValue = false;
            acc.timestamp = Long.MAX_VALUE;
        }
    }
} 