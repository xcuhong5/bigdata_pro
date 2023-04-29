package com.xc.day03_producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 * DateTime: 2022-11-13 15:41
 * 调用 kafka 的 ack 应答机制；
 * ack=0 无需应答 一直发数据
 * ack=1 leader收到数据后应答，接着发
 * ack=-1 就是all ，leader 和 isr 里的follower 都收到数据后应答，再接着发
 */
public class Producer_acks {

    // 设置 ack 应答 机制
    public static void ack_send() {
        // 1. 创建 kafka 配置对象
        Properties conf = new Properties();
        // 2. 配置 连接集群
        conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp100:9092,hdp103:9092,hdp104:9092");
        // 3. 配置 key value 序列化
        conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 4. 配置 ack
        conf.put(ProducerConfig.ACKS_CONFIG, "-1");
        // 5. 配置 发送 重试 次数, 重试 3次
        conf.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 6. 主题名
        String topic = "first_tpc";
        // 7. 创建 producer 对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(conf);
        // 8. 发送消息
        for (int i = 0; i <= 10; i++) {
            producer.send(new ProducerRecord<>(topic, "Producer_acks:" + i));
        }
        // 9. 关闭
        producer.close();
    }

    public static void main(String[] args) {
        ack_send();
    }
}
