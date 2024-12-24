package com.stonewu.flink.udf.aggregation;

import org.junit.Test;
import static org.junit.Assert.*;

public class LastValueFunctionTest {
    
    @Test
    public void testLastValueWithTimestamp() {
        LastValueFunction<String> function = new LastValueFunction<>();
        LastValueFunction.LastValueAccumulator<String> acc = function.createAccumulator();
        
        // 按时间顺序添加数据
        function.accumulate(acc, "first", 1000L);
        assertEquals("first", function.getValue(acc));
        
        function.accumulate(acc, "second", 2000L);
        assertEquals("second", function.getValue(acc));
        
        // 添加更早的数据，不应影响结果
        function.accumulate(acc, "earlier", 500L);
        assertEquals("second", function.getValue(acc));
        
        // 添加最晚的数据
        function.accumulate(acc, "latest", 3000L);
        assertEquals("latest", function.getValue(acc));
    }
    
    @Test
    public void testMergeAccumulators() {
        LastValueFunction<String> function = new LastValueFunction<>();
        
        // 创建第一个累加器
        LastValueFunction.LastValueAccumulator<String> acc1 = function.createAccumulator();
        function.accumulate(acc1, "acc1-value", 1000L);
        
        // 创建第二个累加器，时间戳更晚
        LastValueFunction.LastValueAccumulator<String> acc2 = function.createAccumulator();
        function.accumulate(acc2, "acc2-value", 2000L);
        
        // 创建第三个累加器，时间戳最晚
        LastValueFunction.LastValueAccumulator<String> acc3 = function.createAccumulator();
        function.accumulate(acc3, "acc3-value", 3000L);
        
        // 创建目标累加器
        LastValueFunction.LastValueAccumulator<String> targetAcc = function.createAccumulator();
        
        // 合并所有累加器
        function.merge(targetAcc, java.util.Arrays.asList(acc1, acc2, acc3));
        
        // 验证结果是否为最晚时间戳的值
        assertEquals("acc3-value", function.getValue(targetAcc));
        assertEquals(3000L, targetAcc.timestamp);
    }
    
    @Test
    public void testNullValues() {
        LastValueFunction<String> function = new LastValueFunction<>();
        LastValueFunction.LastValueAccumulator<String> acc = function.createAccumulator();
        
        // 测试空累加器
        assertNull("空累加器应返回null", function.getValue(acc));
        assertFalse("新累加器hasValue应为false", acc.hasValue);
        assertEquals("新累加器timestamp应为Long.MIN_VALUE", Long.MIN_VALUE, acc.timestamp);
        
        // 测试null值
        function.accumulate(acc, null, 1000L);
        assertNull(function.getValue(acc));
        assertTrue(acc.hasValue);
        assertEquals(1000L, acc.timestamp);
        
        // 确保null值也遵循时间戳规则
        function.accumulate(acc, "notNull", 2000L);
        assertEquals("notNull", function.getValue(acc));
        assertEquals(2000L, acc.timestamp);
    }
    
    @Test
    public void testEdgeCases() {
        LastValueFunction<String> function = new LastValueFunction<>();
        LastValueFunction.LastValueAccumulator<String> acc = function.createAccumulator();
        
        // 测试相同时间戳的值
        function.accumulate(acc, "value1", 1000L);
        function.accumulate(acc, "value2", 1000L);
        assertEquals("value1", function.getValue(acc));
        assertEquals(1000L, acc.timestamp);
        
        // 测试时间戳为0的情况
        function.accumulate(acc, "zero", 0L);
        assertEquals("value1", function.getValue(acc));  // 因为0 < 1000，所以不会更新
        
        // 测试时间戳为Long.MAX_VALUE的情况
        function.accumulate(acc, "max", Long.MAX_VALUE);
        assertEquals("max", function.getValue(acc));
        assertEquals(Long.MAX_VALUE, acc.timestamp);
    }
    
    @Test
    public void testComplexMergeScenarios() {
        LastValueFunction<String> function = new LastValueFunction<>();
        
        // 创建多个累加器测试不同场景
        LastValueFunction.LastValueAccumulator<String> acc1 = function.createAccumulator();
        LastValueFunction.LastValueAccumulator<String> acc2 = function.createAccumulator();
        LastValueFunction.LastValueAccumulator<String> acc3 = function.createAccumulator();
        LastValueFunction.LastValueAccumulator<String> emptyAcc = function.createAccumulator();
        
        // 设置不同的值和时间戳
        function.accumulate(acc1, "value1", 1000L);
        function.accumulate(acc2, "value2", 2000L);
        function.accumulate(acc3, "value3", 3000L);
        
        // 测试合并包含空累加器的情况
        LastValueFunction.LastValueAccumulator<String> targetAcc1 = function.createAccumulator();
        function.merge(targetAcc1, java.util.Arrays.asList(acc1, emptyAcc, acc2));
        assertEquals("value2", function.getValue(targetAcc1));
        assertEquals(2000L, targetAcc1.timestamp);
        
        // 测试所有累加器都为空的情况
        LastValueFunction.LastValueAccumulator<String> targetAcc2 = function.createAccumulator();
        function.merge(targetAcc2, java.util.Arrays.asList(emptyAcc, function.createAccumulator()));
        assertNull(function.getValue(targetAcc2));
        assertEquals(Long.MIN_VALUE, targetAcc2.timestamp);
    }
    
    @Test
    public void testRetract() {
        LastValueFunction<String> function = new LastValueFunction<>();
        LastValueFunction.LastValueAccumulator<String> acc = function.createAccumulator();
        
        // 添加初始值
        function.accumulate(acc, "first", 1000L);
        assertEquals("first", function.getValue(acc));
        assertEquals(1000L, acc.timestamp);
        
        // 撤回不匹配的值和时间戳,不应影响结果
        function.retract(acc, "first", 2000L);
        assertEquals("first", function.getValue(acc));
        assertEquals(1000L, acc.timestamp);
        
        // 撤回匹配的值和时间戳
        function.retract(acc, "first", 1000L);
        assertNull(function.getValue(acc));
        assertFalse(acc.hasValue);
        assertEquals(Long.MIN_VALUE, acc.timestamp);
    }
    
    @Test 
    public void testNullTimestamp() {
        LastValueFunction<String> function = new LastValueFunction<>();
        LastValueFunction.LastValueAccumulator<String> acc = function.createAccumulator();
        
        // 测试timestamp为null的情况
        function.accumulate(acc, "value", null);
        assertNull(function.getValue(acc));
        assertFalse(acc.hasValue);
        assertEquals(Long.MIN_VALUE, acc.timestamp);
    }
    
    @Test
    public void testRetractWithNull() {
        LastValueFunction<String> function = new LastValueFunction<>();
        LastValueFunction.LastValueAccumulator<String> acc = function.createAccumulator();
        
        // 添加null值
        function.accumulate(acc, null, 1000L);
        assertNull(function.getValue(acc));
        assertTrue(acc.hasValue);
        assertEquals(1000L, acc.timestamp);
        
        // 撤回null值
        function.retract(acc, null, 1000L); 
        assertNull(function.getValue(acc));
        assertFalse(acc.hasValue);
        assertEquals(Long.MIN_VALUE, acc.timestamp);
    }
} 