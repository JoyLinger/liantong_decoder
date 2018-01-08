package io.transwarp.util;

import io.transwarp.decoder.MessageDecoder;
import io.transwarp.kafka.SingleProducer;
import kafka.producer.KeyedMessage;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by root on 8/4/17.
 * Send message to kafka.
 */
public class Messager {

  private static Logger LOGGER = Logger.getLogger(Messager.class);
  private static InputStream is = MessageDecoder.class.getClassLoader().getResourceAsStream("topic.properties");
  private static InputStream pis = MessageDecoder.class.getClassLoader().getResourceAsStream("param.properties");
  private static kafka.javaapi.producer.Producer<String, String> producer = SingleProducer.getInstance();
  private static Properties topicProp = new Properties();
  private static Properties paramProp = new Properties();
  private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  static {
    try {
      topicProp.load(is);
      paramProp.load(pis);
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
    }
  }

  public static void sendErrorMsg2RecordMonitor(String errorType, String errorData) {
    // RecordMonitor's topic name.
    String topicName = topicProp.getProperty("11_11");
    String ErrorFlag = paramProp.getProperty("ErrorFlag");
    String msg = errorType + "@@" + errorData + "@@" + sdf.format(new Date()) + "@@" + ErrorFlag;
    producer.send(new KeyedMessage<>(topicName, UUID.randomUUID().toString(), msg));
  }

}
