package io.transwarp.decoder;

import io.transwarp.streaming.sql.api.decoder.ArrayDecoder;
import io.transwarp.util.Messager;
import kafka.utils.VerifiableProperties;

/**
 * Created by zaish on 2016-9-12.
 */
public class VoiceDecoder extends ArrayDecoder<byte[]> {
  public VoiceDecoder(VerifiableProperties props) {
    super(props);
  }

  @Override
  public byte[][] arrayFromBytes(byte[] bytes) {
    String str = new String(bytes);
    if (str.length() != 141) {
//            Messager.sendErrorMsg2RecordMonitor("decoder", str);
      return null;
//            return new byte[][]{("err"+str.length()).getBytes()};
    } else {
      int phoneType_len = 2;
      int callPhone_len = 28;
      int calledPhone_len = 28;
      int startTime_len = 14;
      int sendTime_len = 14;
      int IMSI_len = 16;
      int IMEI_len = 16;
      int LAC_len = 6;
      int CELL_ID_len = 17;
      String phoneType = str.substring(0, phoneType_len).trim();
      String callPhone = str.substring(phoneType_len, phoneType_len + callPhone_len).trim();
      String calledPhone = str.substring(phoneType_len + callPhone_len, phoneType_len + callPhone_len + calledPhone_len).trim();
      String startTime = str.substring(phoneType_len + callPhone_len + calledPhone_len, phoneType_len + callPhone_len + calledPhone_len + startTime_len).trim();
      String sendTime = str.substring(phoneType_len + callPhone_len + calledPhone_len + startTime_len,
              phoneType_len + callPhone_len + calledPhone_len + startTime_len + sendTime_len).trim();
      String IMSI = str.substring(phoneType_len + callPhone_len + calledPhone_len + startTime_len + sendTime_len,
              phoneType_len + callPhone_len + calledPhone_len + startTime_len + sendTime_len + IMSI_len).trim();
      String IMEI = str.substring(phoneType_len + callPhone_len + calledPhone_len + startTime_len + sendTime_len + IMSI_len,
              phoneType_len + callPhone_len + calledPhone_len + startTime_len + sendTime_len + IMSI_len + IMEI_len).trim();
      String LAC = str.substring(phoneType_len + callPhone_len + calledPhone_len + startTime_len + sendTime_len + IMSI_len + IMEI_len,
              phoneType_len + callPhone_len + calledPhone_len + startTime_len + sendTime_len + IMSI_len + IMEI_len + LAC_len).trim();
      String CELL_ID = str.substring(phoneType_len + callPhone_len + calledPhone_len + startTime_len + sendTime_len + IMSI_len + IMEI_len + LAC_len,
              phoneType_len + callPhone_len + calledPhone_len + startTime_len + sendTime_len + IMSI_len + IMEI_len + LAC_len + CELL_ID_len).trim();
      StringBuilder sb = new StringBuilder();
      sb.append(phoneType).append("|").append(callPhone).append("|").append(calledPhone).append("|").append(startTime).append("|")
              .append(sendTime).append("|").append(IMSI).append("|").append(IMEI).append("|").append(LAC).append("|").append(CELL_ID);
      return new byte[][]{sb.toString().getBytes()};
    }
  }
}
