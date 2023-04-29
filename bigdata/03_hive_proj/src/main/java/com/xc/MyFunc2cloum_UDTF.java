package com.xc;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: sky
 * DateTime: 2022-11-06 14:11
 * 实现  炸裂 2个 列
 * 需求： hello,sky:hello,lisa
 * 变成
 * hello sky
 * hello lisa
 */
public class MyFunc2cloum_UDTF extends GenericUDTF {
    private List<String> outList = new ArrayList<>();

    // 数据效验
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 装 列名
        List<String> fieldNames = new ArrayList<>();
        // 装列类型
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        // 添加列名
        fieldNames.add("word1");
        fieldNames.add("word2");
        // 添加 类类型，2个列 对应2个
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    // 逻辑处理
    @Override
    public void process(Object[] args) throws HiveException {
        // 获取 参数，原 数据
        String input = args[0].toString();
        // 根据 ： 分割
        String[] split = input.split(":");
        // 遍历 然后根据 ， 分割
        for (String v : split) {
            String[] word = v.split(",");
            outList.clear();
            // 此时一行 数据 被分割成 2列，分别是 hello sky ； 下一次执行 for 则是 产生新 的一行
            outList.add(word[0]);
            outList.add(word[1]);
            // 输出一次就是 一行 数据
            forward(outList);
        }

    }

    @Override
    public void close() throws HiveException {

    }
}
