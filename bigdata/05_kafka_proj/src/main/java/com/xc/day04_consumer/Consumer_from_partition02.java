package com.xc.day04_consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 * DateTime: 2022-11-14 16:48
 * 消费 指定分区的 数据
 */
public class Consumer_from_partition02 {

    // 消费 指定分区的  数据；生产者 测试 对应 Sync_Producer02
    public static void consumer_partition() {
        // 1. 创建 配置 对象
        Properties conf = new Properties();
        // 2. 连接 集群
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp100:9092,hdp103:9092,hdp104:9092");
        // 3. key value 反序列化
        conf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        conf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 设置 消费组、
        conf.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        // 4. 创建 消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(conf);
        // 5. 设置 消费 指定 主题 和 分区的数据
        List<TopicPartition> topicPartitions = new ArrayList<>();
        // 设置主题 和 主题相应的 分区 (消费 主题 分区号2 的数据)
        topicPartitions.add(new TopicPartition("first_tpc", 2));
        // 6. 分配 主题分区 到 消费者
        consumer.assign(topicPartitions);
        while (true) {
            // 7. 消费数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
    }

    public static void main(String[] args) {
        consumer_partition();
    }
}
