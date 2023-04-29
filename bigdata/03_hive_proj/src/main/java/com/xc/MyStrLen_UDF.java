package com.xc;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 * DateTime: 2022-11-06 9:46
 * 自定义 UDF 函数 一进一出 ；继承 GenericUDF
 * 需求： 计算 指定 字符串的  长度
 */
public class MyStrLen_UDF extends GenericUDF {
    /**
     * 初始化 方法；校验 参数的 个数 和 类型 作用
     *
     * @param arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 判断 参数 个数
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("参数 个数 异常！！");
        }
        // 判断 参数  的 类型
        if (!arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
            throw new UDFArgumentTypeException(0, "参数类型 异常 ! !");
        }
        //返回 参数 的 类型
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 处理 数据  业务逻辑
     *
     * @param arguments
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        // 取出 数据
        String input = arguments[0].get().toString();
        // 判断 参数是否 为 null
        if (StringUtils.isEmpty(input)) {
            return 0;
        }
        // 返回 长度
        return input.length();
    }

    @Override
    public String getDisplayString(String[] children) {
        return null;
    }
}
