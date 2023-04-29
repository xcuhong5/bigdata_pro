package com.xc.day05_commitOffset;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 * DateTime: 2022-11-15 15:08
 * 指定 offset 消费数据
 */
public class Appoint_offset {
    // 指定 相应 的 offset 消费
    public static void appoint() {
        // 1. 创建 配置对象
        Properties conf = new Properties();
        // 2. 连接集群
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp100:9092,hdp103:9092,hdp104:9092");
        // 3. key value 反序列化
        conf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        conf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 消费组 id
        conf.put(ConsumerConfig.GROUP_ID_CONFIG, "test9");
        // 4. 创建 生产者 对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(conf);
        // 5. 订阅主题
        consumer.subscribe(Arrays.asList("first_tpc"));
        // 5. 获取 消费者 分区 分配 信息
        Set<TopicPartition> partitions = consumer.assignment();
        // 6. 可能程序太快，获取的分区是空的，试探性拉一下数据，重新回去分区信息
        while (partitions.size() == 0) {
            // 试探性拉数据，方便 获取分区信息
            consumer.poll(Duration.ofSeconds(1));
            // 重新 获取分区信息
            partitions = consumer.assignment();
        }
        // 7. 遍历 获取的 分区
        for (TopicPartition topicPartition : partitions) {
            // 指定分区 指定offset
            consumer.seek(topicPartition, 361016);
        }
        // 8. 开始消费
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
    }


    public static void main(String[] args) {
        appoint();
    }
}
