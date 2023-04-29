package com.xc.day04_consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 * DateTime: 2022-11-14 15:38
 * 消费者 api， 一个消费者的案列
 */
public class Consumer01 {

    public static void consumer_msg() {
        // 1. 创建 配置对象
        Properties conf = new Properties();
        // 2. 配置 连接 集群
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp100:9092,hdp103:9092,hdp104:9092");
        // 3. key value 反序列化
        conf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        conf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 消费组id
        conf.put(ConsumerConfig.GROUP_ID_CONFIG, "t8");
        // 4. 主题名
        ArrayList<String> topics = new ArrayList<>();
        topics.add("first_tpc");
        // 5. 创建 消费者 对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(conf);
        // 6. 订阅 主题
        consumer.subscribe(topics);
        // 7. 消费数据
        while (true) {
            // 每 1ms 拉一次数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("first_tpc 消费数据 ：" + record);
            }
        }
    }

    public static void main(String[] args) {
        consumer_msg();
    }
}
