package com.xc.day05_commitOffset;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 * DateTime: 2022-11-15 15:08
 * 指定 时间  消费数据
 */
public class Appoint_time {
    // 指定 消费昨天的数据 消费
    public static void appoint_time() {
        // 1. 创建 配置对象
        Properties conf = new Properties();
        // 2. 连接集群
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp100:9092,hdp103:9092,hdp104:9092");
        // 3. key value 反序列化
        conf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        conf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 消费组 id
        conf.put(ConsumerConfig.GROUP_ID_CONFIG, "cc");
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

        Map<TopicPartition, Long> topicPartition_time = new HashMap<>();
        // 7. 遍历 获取的 分区，将分区 和 时间 封装到map
        for (TopicPartition topicPartition : partitions) {
            // 将 分区 和 消费的 时间戳 装进 集合 ,消费昨天的数据
            topicPartition_time.put(topicPartition, System.currentTimeMillis()-1*24*3600*1000);
        }
        // 8. 调用 根据时间 找到 offset 的函数
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = consumer.offsetsForTimes(topicPartition_time);

        // 9. 遍历分区，根据分区 get 在 offsetAndTimestampMap 中的offset
        for (TopicPartition topicPartition : partitions) {
            // 获取分区 对应的 offset
            OffsetAndTimestamp timestamp = offsetAndTimestampMap.get(topicPartition);
            // 避免空值 报错
            if (null != timestamp) {
                // 指定分区 指定offset
                consumer.seek(topicPartition, timestamp.offset());
            }
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
        appoint_time();
    }
}
