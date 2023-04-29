package com.xc.day05_commitOffset;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 * DateTime: 2022-11-15 14:21
 * 消费者 自动提交 offset
 */
public class AutoCommit {
    public static void consumer_offset() {
        // 1. 创建 配置对象
        Properties conf = new Properties();
        // 连接 集群
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hdp100:9092,hdp103:9092,hdp104:9092");
        // 2. 配置 key value 反序列化
        conf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        conf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 3. 自动 提交 offset 默认的 ，修改一下 间隔时间
        conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 自动提交 时间 间隔 1000ms 默认是5s
        conf.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        // 消费组
        conf.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        // 4. 创建 消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(conf);
        // 5. 设置 消费的主题
        consumer.subscribe(Arrays.asList("first_tpc"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
    }

    public static void main(String[] args) {
        consumer_offset();
    }
}
