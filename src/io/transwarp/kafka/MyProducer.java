package io.transwarp.kafka;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by zaish on 2016-10-21.
 */
public class MyProducer extends Thread {
  private final kafka.javaapi.producer.Producer<String, String> producer;
  private final String topic;

  public MyProducer(String topic, String brokers) {
    Properties props = new Properties();
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("metadata.broker.list", brokers);
    props.put("request.required.acks", "1");

    producer = new kafka.javaapi.producer.Producer<String, String>(new ProducerConfig(props));
    this.topic = topic;
  }

  public void run() {
//        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
////        singleThreadExecutor.execute();
//              try {
//
//                producer.send(new KeyedMessage<String, String>(topic,message);
//                Thread.sleep(7000);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
////            System.out.println(123);
//            producer.close();
//        }
  }
}
