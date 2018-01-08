package io.transwarp.kafka;

import kafka.producer.ProducerConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by zaish on 2016-10-23.
 */
public class SingleProducer {
  private static kafka.javaapi.producer.Producer<String, String> producer;

  private SingleProducer() {
  }

  //静态工厂方法
  public static kafka.javaapi.producer.Producer<String, String> getInstance() {
    InputStream is = SingleProducer.class.getClassLoader().getResourceAsStream("resources" + File.separator + "kafka.properties");
    Properties p = new Properties();
    try {
      p.load(is);
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (producer == null) {
      producer = new kafka.javaapi.producer.Producer<String, String>(new ProducerConfig(p));
    }
    return producer;
  }
}
