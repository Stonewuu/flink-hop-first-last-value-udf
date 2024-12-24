package com.stonewu.flink.udf.aggregation;

import org.junit.Test;
import static org.junit.Assert.*;
import java.util.Arrays;

public class FirstValueFunctionTest {
    
    @Test
    public void testBasicFirstValue() {
        FirstValueFunction<String> function = new FirstValueFunction<>();
        FirstValueFunction.FirstValueAccumulator<String> acc = function.createAccumulator();
        
        // 测试空累加器
        assertNull("空累加器应返回null", function.getValue(acc));
        assertFalse("新累加器hasValue应为false", acc.hasValue);
        assertEquals("新累加器timestamp应为Long.MAX_VALUE", Long.MAX_VALUE, acc.timestamp);
        
        // 测试单个值
        function.accumulate(acc, "first", 100L);
        assertEquals("first", function.getValue(acc));
        assertEquals(100L, acc.timestamp);
        
        // 测试更早时间戳的值
        function.accumulate(acc, "earlier", 50L);
        assertEquals("earlier", function.getValue(acc));
        assertEquals(50L, acc.timestamp);
        
        // 测试更晚时间戳的值（不应更新）
        function.accumulate(acc, "later", 150L);
        assertEquals("earlier", function.getValue(acc));
        assertEquals(50L, acc.timestamp);
    }
    
    @Test
    public void testMerge() {
        FirstValueFunction<String> function = new FirstValueFunction<>();
        FirstValueFunction.FirstValueAccumulator<String> acc1 = function.createAccumulator();
        FirstValueFunction.FirstValueAccumulator<String> acc2 = function.createAccumulator();
        FirstValueFunction.FirstValueAccumulator<String> acc3 = function.createAccumulator();
        
        // 设置不同时间戳的值
        function.accumulate(acc1, "value1", 100L);
        function.accumulate(acc2, "value2", 50L);
        function.accumulate(acc3, "value3", 150L);
        
        // 创建目标累加器
        FirstValueFunction.FirstValueAccumulator<String> targetAcc = function.createAccumulator();
        
        // 合并累加器
        function.merge(targetAcc, java.util.Arrays.asList(acc1, acc2, acc3));
        
        // 验证结果
        assertEquals("value2", function.getValue(targetAcc));
        assertEquals(50L, targetAcc.timestamp);
    }
    
    @Test
    public void testNullValues() {
        FirstValueFunction<String> function = new FirstValueFunction<>();
        FirstValueFunction.FirstValueAccumulator<String> acc = function.createAccumulator();
        
        // 测试null值
        function.accumulate(acc, null, 100L);
        assertNull(function.getValue(acc));
        assertTrue(acc.hasValue);
        
        // 确保null值也遵循时间戳规则
        function.accumulate(acc, "notNull", 50L);
        assertEquals("notNull", function.getValue(acc));
    }
    
    @Test
    public void testEdgeCases() {
        FirstValueFunction<String> function = new FirstValueFunction<>();
        FirstValueFunction.FirstValueAccumulator<String> acc = function.createAccumulator();
        
        // 测试相同时间戳的值
        function.accumulate(acc, "value1", 100L);
        function.accumulate(acc, "value2", 100L);
        assertEquals("value1", function.getValue(acc));
        assertEquals(100L, acc.timestamp);
        
        // 测试时间戳为0的情况
        function.accumulate(acc, "zero", 0L);
        assertEquals("zero", function.getValue(acc));
        assertEquals(0L, acc.timestamp);
        
        // 测试时间戳为Long.MIN_VALUE的情况
        function.accumulate(acc, "min", Long.MIN_VALUE);
        assertEquals("min", function.getValue(acc));
        assertEquals(Long.MIN_VALUE, acc.timestamp);
    }
    
    @Test
    public void testComplexMergeScenarios() {
        FirstValueFunction<String> function = new FirstValueFunction<>();
        
        // 创建多个累加器测试不同场景
        FirstValueFunction.FirstValueAccumulator<String> acc1 = function.createAccumulator();
        FirstValueFunction.FirstValueAccumulator<String> acc2 = function.createAccumulator();
        FirstValueFunction.FirstValueAccumulator<String> acc3 = function.createAccumulator();
        FirstValueFunction.FirstValueAccumulator<String> emptyAcc = function.createAccumulator();
        
        // 设置不同的值和时间戳
        function.accumulate(acc1, "value1", 100L);
        function.accumulate(acc2, "value2", 50L);
        function.accumulate(acc3, "value3", 150L);
        
        // 测试合并包含空累加器的情况
        FirstValueFunction.FirstValueAccumulator<String> targetAcc1 = function.createAccumulator();
        function.merge(targetAcc1, Arrays.asList(acc1, emptyAcc, acc2));
        assertEquals("value2", function.getValue(targetAcc1));
        assertEquals(50L, targetAcc1.timestamp);
        
        // 测试所有累加器都为空的情况
        FirstValueFunction.FirstValueAccumulator<String> targetAcc2 = function.createAccumulator();
        function.merge(targetAcc2, Arrays.asList(emptyAcc, function.createAccumulator()));
        assertNull(function.getValue(targetAcc2));
        assertEquals(Long.MAX_VALUE, targetAcc2.timestamp);
    }
    
    @Test
    public void testRetract() {
        FirstValueFunction<String> function = new FirstValueFunction<>();
        FirstValueFunction.FirstValueAccumulator<String> acc = function.createAccumulator();
        
        // 添加初始值
        function.accumulate(acc, "first", 100L);
        assertEquals("first", function.getValue(acc));
        assertEquals(100L, acc.timestamp);
        
        // 撤回不匹配的值和时间戳,不应影响结果
        function.retract(acc, "first", 200L);
        assertEquals("first", function.getValue(acc));
        assertEquals(100L, acc.timestamp);
        
        // 撤回匹配的值和时间戳
        function.retract(acc, "first", 100L);
        assertNull(function.getValue(acc));
        assertFalse(acc.hasValue);
        assertEquals(Long.MAX_VALUE, acc.timestamp);
    }
    
    @Test 
    public void testNullTimestamp() {
        FirstValueFunction<String> function = new FirstValueFunction<>();
        FirstValueFunction.FirstValueAccumulator<String> acc = function.createAccumulator();
        
        // 测试timestamp为null的情况
        function.accumulate(acc, "value", null);
        assertNull(function.getValue(acc));
        assertFalse(acc.hasValue);
        assertEquals(Long.MAX_VALUE, acc.timestamp);
    }
    
    @Test
    public void testRetractWithNull() {
        FirstValueFunction<String> function = new FirstValueFunction<>();
        FirstValueFunction.FirstValueAccumulator<String> acc = function.createAccumulator();
        
        // 添加null值
        function.accumulate(acc, null, 100L);
        assertNull(function.getValue(acc));
        assertTrue(acc.hasValue);
        assertEquals(100L, acc.timestamp);
        
        // 撤回null值
        function.retract(acc, null, 100L); 
        assertNull(function.getValue(acc));
        assertFalse(acc.hasValue);
        assertEquals(Long.MAX_VALUE, acc.timestamp);
    }
} 