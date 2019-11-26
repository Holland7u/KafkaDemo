package com.caixun.zy_group;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

public class Producer extends Thread{
//    定制生产者对象
    private final KafkaProducer<Integer,String> producer;
    //定制主题topic
    private final String topic;

    public Producer(String topic) {
        //使用Properties，线程安全
        Properties prop = new Properties();
        //连接字符串
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.25.131:9092");
        //设置客户端Id
        prop.put(ProducerConfig.CLIENT_ID_CONFIG,"0");
        //key的序列化
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,IntegerSerializer.class.getName());
        //Value的序列化
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //批量发送大小，生产者发送多条消息给kafka时，为了减少请求，采用批量发送的方式
        //相当于创建一个缓冲区，当缓冲区满时再统一发送给kafka，缓冲区大小为16kb；
        prop.put(ProducerConfig.BATCH_SIZE_CONFIG,16*1024);
        //设置批量发送的时间间隔
        prop.put(ProducerConfig.LINGER_MS_CONFIG,1000);

        producer = new KafkaProducer<Integer, String>(prop);
        this.topic = topic;
    }

    @Override
    public void run() {
        //1.定义循环条件
        int num = 0;
        //2.开始循环
        while (num<=50){
            //3.定义发送给Kafka的消息
            String msg = "测试消息-第"+num+"条";
            try {
                //发送消息，并定义回调函数用来打印信息
            producer.send(new ProducerRecord<>(topic, msg), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("callback"+metadata.offset()+"--->brokerId"+metadata.partition()
                    +"--->msg='"+metadata.toString()+"'");
                }
            });
            //发送一次等1.5秒
                Thread.sleep(1500);
                num++;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        new Producer("testLog").start();
    }
}
