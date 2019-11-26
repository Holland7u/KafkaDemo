package com.caixun.zy_group;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer extends Thread {
    private final KafkaConsumer<Integer,String> consumer;
    private final String topic;

    public Consumer(String topic) {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.25.131:9092");
        //设置组名（内容是啥无所谓，组内统一就行，但是不能少）
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,"testConsumer");
        //设置消费时自动提交Offset
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        //设置自动提交时间
        prop.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        //设置消息的超时时间
        prop.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"30000");
        //设置消费者key和value的反序列化方式
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.IntegerDeserializer");
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        //设置Offset的默认值
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        this.consumer = new KafkaConsumer<Integer, String>(prop);
        this.topic = topic;
    }


    @Override
    public void run() {

        while (true){
            //创建这个topic的单例，作为消费的主题
            consumer.subscribe(Collections.singleton(this.topic));
            //通过consumer对象拉取消息
            ConsumerRecords<Integer,String> records =
                    consumer.poll(Duration.ofSeconds(1));
            //解析拉取下来的消息
            records.forEach(record->{
                System.out.println(record.key()+"--offset:"+record.offset()+
                "--val:"+record.value());
            });
        }
    }

    public static void main(String[] args) {
        new Consumer("testLog").start();
    }
}
