package com.xc.day04_consumer.groups;

import com.xc.day02_producer.CustomPartitioner01;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 * DateTime: 2022-11-12 20:45
 * 为 consumer_group01-03  做测试
 * 调用 自定义 分区; 自定义的分区号 要必须存在，否则会一直等待 对应的分区
 */
public class Producer_group{
    //调用 自定义 分区器 实现 生产者 按照 相应 分区 发送消息
    public static void custom_partition_send_msg() {
        // 1. 创建 生产者 配置 对象
        Properties conf = new Properties();
        // 2. 配置 集群访问
        conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp100:9092,hdp103:9092,hdp104:9092");
        // 3. 配置 key 和 value 的序列化
        conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 4. 关联自定义分区, 关联 自定义分区的类
        conf.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner01.class.getName());

        // 5. 主题名
        String topic = "first_tpc";
        // 6. 创建 生产者 对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(conf);
        StringBuffer msg = null;
        for (int i = 0; i <= 10000; i++) {
            msg = new StringBuffer();
            if (i % 3 == 0) {
                msg.append("xc_");
                msg.append(i);
            } else if (i % 3 == 1) {
                msg.append("sky_");
                msg.append(i);
            } else {
                msg.append("other_");
                msg.append(i);
            }
            // 7. 发送消息
            producer.send(new ProducerRecord<>(topic, msg.toString()), new Callback() {
                // 回调函数 ，查看 自定义分区 结果
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("主题：" + metadata.topic() + "  分区：" + metadata.partition() + "   msg:" + metadata.serializedValueSize());
                    } else {
                        System.out.println("异常：" + exception.toString());
                    }
                }
            });
        }
        //8 . 关闭资源
        producer.close();
    }

    public static void main(String[] args) {
        // 调用 应用 自定义分区 的 生产者 发消息
        custom_partition_send_msg();
    }
}
