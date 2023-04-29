package com.xc.day01_producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 * DateTime: 2022-11-11 20:51
 * kafka 异步 发送消息
 */
public class Async_Producer01 {
    // 异步发送消息 不带回调函数
    public static void async_send() {
        // 1. 创建 kafka 生产者 的配置对象
        Properties conf = new Properties();
        // 2. 给kafka 配置对象 添加配置信息 就是 shell 的 --bootstrap-server
        conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp100:9092,hdp103:9092,hdp104:9092");
        // 3. key value 需要 序列化
        conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 4. 主题名
        String topicName = "first_tpc";
        // 5. 创建 kafka 生产者 对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(conf);
        // 6. 发送 消息
        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>(topicName, "async_xc:" + i));
        }
        // 7. 关闭资源
        producer.close();
    }

    // 异步发送消息 ，带 回调函数
    public static void async_send_callback() {
        // 1. 创建 kafka 生产者 的配置对象
        Properties conf = new Properties();
        // 2. 给kafka 配置对象 添加配置信息 就是 shell 的 --bootstrap-server
        conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp100:9092,hdp103:9092,hdp104:9092");
        // 3. key value 需要 序列化
        conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 4. 主题名
        String topicName = "first_tpc";
        // 5. 创建 kafka 生产者 对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(conf);
        // 6. 发送 消息
        for (int i = 0; i < 10; i++) {
            // 7. 回调函数 Callback，数据 发送后 返回的信息，可以在 回调函数中 操作
            producer.send(new ProducerRecord<>(topicName, "async_callback_sky:" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // 异常 为空 则 发送成功
                    if (exception == null) {
                        System.out.println("主题：" + metadata.topic() + "    分区：" + metadata.partition());
                    }
                }
            });
        }
        // 7. 关闭资源
        producer.close();
    }

    public static void main(String[] args) {
        // 调用 无回调函数 的 异步发送
        async_send();
        // 调用 异步发送消息 带 回调函数
        async_send_callback();
    }
}
