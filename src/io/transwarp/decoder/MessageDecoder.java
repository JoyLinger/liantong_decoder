package io.transwarp.decoder;

import io.transwarp.streaming.sql.api.decoder.ArrayDecoder;
import io.transwarp.util.Messager;
import kafka.utils.VerifiableProperties;


/**
 * Created by zaish on 2016-9-12.
 */
public class MessageDecoder extends ArrayDecoder<byte[]> {
  public MessageDecoder(VerifiableProperties props) {
    super(props);
  }

  @Override
  public byte[][] arrayFromBytes(byte[] bytes) {
    String str = new String(bytes);
    if (str.length() != 101) {
//            Messager.sendErrorMsg2RecordMonitor("decoder", str);
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
