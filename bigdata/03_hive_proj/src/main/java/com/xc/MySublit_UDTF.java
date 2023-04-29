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
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 * DateTime: 2022-11-06 10:39
 * 自定义函数 UDTF ; 一进多出
 * 需求 ： 实现 字符的任意分割
 */
public class MySublit_UDTF extends GenericUDTF {
    private List<String> outList = new ArrayList<>();

    /**
     * 单独 重写的 ，做数据效验，参数个数 和 类型
     *
     * @param argOIs
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 1. 定义 装 列名 的集合
        List<String> fieldNames = new ArrayList<>();
        // 2. 定义 列的类型
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        // 3.添加列名 到集合
        fieldNames.add("lineToWord");
        // 4. 添加 列 类型
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);

    }

    // 处理 输入数据
    @Override
    public void process(Object[] args) throws HiveException {
        // 获取 第一个 参数, 是字段
        String value = args[0].toString();
        // 获取第二个参数，是分割符
        String sub = args[1].toString();
        String[] strings = value.split(sub);
        // 遍历 数组 内容 进行输出
        for (String val : strings) {
            //装进 list 之前 先 清空
            outList.clear();
            // 装进 list
            outList.add(val);
            // 写出 数据；输出；  输出一次就是 一行 数据
            forward(outList);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
