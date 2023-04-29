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
 * 这个 是作为生产者 配合 sparkStreaming 消费数据
 * 生产者 提高 吞吐量，参数 调优
 * barch.size ： 发送数据 批次大小默认是 16k；可以修改 为32k
 * linger.ms ：等待 发送批次数据的 时间，默认是0，缓冲区来一条就发一条到集群；
 * 可以修改5-100ms（这个值越大 数据的延迟越大）
 * compresisson.type: 使用  压缩 snappy，发送的压缩数据
 * RecordAccumultor： 缓冲区 大小； 默认 32m 可以修改为 64m
 */
public class Producer_tuning_SparkStreaming {
    public static void producer_send() throws InterruptedException {
        // 1. 创建 kafka 配置 对象
        Properties conf = new Properties();
        // 2. 配置 集群连接
        conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp180:9092,hdp181:9092,hdp182:9092");
        // 3. 配置 key value 序列化
        conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /* 4.   生产者 调优 */
        // 发送批次数据 调整 32k
        conf.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        // 调整 批次发送 等待时长 5-100ms
        conf.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        // 设置 数据压缩类型  snappy; 默认 none，可配置值 gzip、snappy、 lz4和zstd
        conf.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        // 设置缓冲区 大小 默认 32m，改64m
        conf.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);

        // 5. 主题名
        String topicName = "spark_streaming001";
        // 6. 创建 producer 对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(conf);
        // 7. 发送消息
        for (int i = 0; i <= 1000000000; i++) {
            producer.send(new ProducerRecord<>(topicName, "Producer_tuning:" + i));
            System.out.println(i);
            Thread.sleep(500);
        }
        // 8. 关闭 生产者
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        // 调用 生产者 参数调优 案列
        producer_send();
    }
}
