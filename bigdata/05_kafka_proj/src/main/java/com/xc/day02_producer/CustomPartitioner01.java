package com.xc.day02_producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @Author: sky
 * DateTime: 2022-11-12 20:34
 * 自定义分区  实现 partitioner 接口 重写 partition()
 * 需求 ：  实现 数据 包含 xc 发 0 分区，包含 sky 发1 分区 ，其他 发2 分区
 */
public class CustomPartitioner01 implements Partitioner {
    /**
     * @param topic      主题 名
     * @param key        数据的 key
     * @param keyBytes   数据 key 序列化 字节数字
     * @param value      数据的value
     * @param valueBytes 数据value 的 序列化 字节数组
     * @param cluster    集群 元数据
     * @return 返回 分区 号
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 1. 获取 数据 转 字符串
        String msg = value.toString();
        // 2. 声明 分区 变量
        int partition;
        /* 3. 判断 数据 所包含的 字符，将数据发送 到相应 的分区
        自定义的分区号 要必须存在，否则会一直等待 对应的分区*/
        if (msg.contains("xc")) {
            partition = 0;
        } else if (msg.contains("sky")) {
            partition = 1;
        } else {
            partition = 2;
        }
        // 4. 返回 分区号
        return partition;
    }

    // 关闭资源
    @Override
    public void close() {

    }

    // 配置信息
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
