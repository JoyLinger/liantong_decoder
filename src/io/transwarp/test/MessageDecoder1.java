package io.transwarp.test;

import io.transwarp.kafka.SingleProducer;
import io.transwarp.streaming.sql.api.decoder.ArrayDecoder;
import kafka.producer.KeyedMessage;
import kafka.utils.VerifiableProperties;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by zaish on 2016-9-12.
 */
public class MessageDecoder1 extends ArrayDecoder<byte[]> {
  public MessageDecoder1(VerifiableProperties props) {
    super(props);
  }

  @Override
  public byte[][] arrayFromBytes(byte[] bytes) {
    String str = new String(bytes);
    if (str.length() != 101) {
      InputStream is = MessageDecoder1.class.getClassLoader().getResourceAsStream("topic.properties");
      kafka.javaapi.producer.Producer<String, String> producer = SingleProducer.getInstance();
      Properties topicProp = new Properties();
      try {
        topicProp.load(is);
      } catch (IOException e) {
        e.printStackTrace();
      }
      String topicName = topicProp.getProperty("11_11");
      producer.send(new KeyedMessage<>(topicName, UUID.randomUUID().toString(), str));
      return null;
//            return new byte[][]{("err"+str.length()).getBytes()};
    } else {
      int phoneType_len = 2;
      int sendPhone_len = 21;
      int recievePhone_len = 21;
      int billPhone_len = 21;
      int sendTime_len = 14;
      int msgLength_len = 22;
      String phoneType = str.substring(0, phoneType_len).trim();
      String sendPhone = str.substring(phoneType_len,
              phoneType_len + sendPhone_len).trim();
      String recievePhone = str.substring(phoneType_len + sendPhone_len,
              phoneType_len + sendPhone_len + recievePhone_len).trim();
      String billPhone = str.substring(phoneType_len + sendPhone_len + recievePhone_len,
              phoneType_len + sendPhone_len + recievePhone_len + billPhone_len).trim();
      String sendTime = str.substring(phoneType_len + sendPhone_len + recievePhone_len + billPhone_len,
              phoneType_len + sendPhone_len + recievePhone_len + billPhone_len + sendTime_len).trim();
      String msgLength = str.substring(phoneType_len + sendPhone_len + recievePhone_len + billPhone_len + sendTime_len,
              phoneType_len + sendPhone_len + recievePhone_len + billPhone_len + sendTime_len + msgLength_len).trim();
      StringBuilder sb = new StringBuilder();
      sb.append(phoneType).append("|").append(sendPhone).append("|").append(recievePhone).append("|").append(billPhone).append("|")
              .append(sendTime).append("|").append(msgLength);
      return new byte[][]{sb.toString().getBytes()};
    }
  }
}
