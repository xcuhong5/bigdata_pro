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
 * DateTime: 2022-11-13 16:32
 * kafka 事务
 * 即使使用 ack 机制 ，无法确保数据不重复，所以引入 事务 确保数据不重复;
 * 主要配置 ： 配置事务 全局唯一id  初始化事务 开启事务 提交事务 异常则放弃事务回滚
 */
public class Producer_transaction {
    public static void transaction_send() {
        // 1. 创建 配置对象
        Properties conf = new Properties();
        // 2. 配置连接 集群
        conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp100:9092,hdp103:9092,hdp104:9092");
        // 3. 配置 key value 序列化
        conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 4. 配置 事务id 的名字，全局唯一
        conf.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "first_tpc_transactionID_0");
        // 5. 主题名
        String topic = "first_tpc";
        // 6. 创建 生产者 对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(conf);
        // 6. 初始化事务
        producer.initTransactions();
        // 7. 开启事务
        producer.beginTransaction();
        // 8. 发送消息
        try {
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<>(topic, "Producer_transaction:" + i));
            }
//            int i = 1 / 0;
            // 9. 提交事务
            producer.commitTransaction();
        } catch (Exception ex) {
            // 10. 如果 提交失败 报错，放弃事务，相当于 回滚,数据就不会发送到jiqun
            producer.abortTransaction();
        } finally {
            // 11. 关闭
            producer.close();
        }
    }

    public static void main(String[] args) {
        transaction_send();
    }
}
