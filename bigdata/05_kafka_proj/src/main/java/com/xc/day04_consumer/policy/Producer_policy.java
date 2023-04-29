package com.xc.day04_consumer.policy;

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
 * 为 Consumer_policy01-03  做测试
 */
public class Producer_policy {
    //调用 自定义 分区器 实现 生产者 按照 相应 分区 发送消息
    public static void custom_partition_send_msg() throws InterruptedException {
        // 1. 创建 生产者 配置 对象
        Properties conf = new Properties();
        // 2. 配置 集群访问
        conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp100:9092,hdp103:9092,hdp104:9092");
        // 3. 配置 key 和 value 的序列化
        conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 5. 主题名
        String topic = "first_tpc";
        // 6. 创建 生产者 对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(conf);

        for (int i = 0; i <= 10000000; i++) {
            // 7. 发送消息
            producer.send(new ProducerRecord<>(topic, "Producer_policy:" + i), new Callback() {
                // 回调函数 ，查看  结果
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("主题：" + metadata.topic() + "  分区：" + metadata.partition() + "   msg:" + metadata.serializedValueSize());
                    } else {
                        System.out.println("异常：" + exception.toString());
                    }
                }
            });
            // 通过等待 让数据 发送到 其他分区
           // Thread.sleep(2);
        }
        //8 . 关闭资源 2 5   1  4  0 3 6
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        custom_partition_send_msg();
    }
}
