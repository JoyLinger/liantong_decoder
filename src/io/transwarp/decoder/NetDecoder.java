package io.transwarp.decoder;

import io.transwarp.streaming.sql.api.decoder.ArrayDecoder;
import io.transwarp.util.Messager;
import kafka.utils.VerifiableProperties;

/**
 * Created by zaish on 2016-9-12.
 */
public class NetDecoder extends ArrayDecoder<byte[]> {
  public NetDecoder(VerifiableProperties props) {
    super(props);
  }

  @Override
  public byte[][] arrayFromBytes(byte[] bytes) {
    String str = new String(bytes);
    if (str.length() != 183) {
//            Messager.sendErrorMsg2RecordMonitor("decoder", str);
      return null;
//            return new byte[][]{("err"+str.length()).getBytes()};
    } else {
      int phoneType_len = 2;
      int charge_phone_len = 24;
      int startTime_len = 14;
      int endTime_len = 14;
      int up_stream_len = 15;
      int down_stream_len = 15;
      int IMSI_len = 16;
      int IMEI_len = 16;
      int LAC_len = 6;
      int CELL_ID_len = 17;
      int SGSN_IP_len = 16;
      int GGSN_IP_len = 16;
      int PLMN_len = 12;
      String phoneType = str.substring(0, phoneType_len).trim();
      String charge_phone = str.substring(phoneType_len,
              phoneType_len + charge_phone_len).trim();
      String startTime = str.substring(phoneType_len + charge_phone_len,
              phoneType_len + charge_phone_len + startTime_len).trim();
      String endTime = str.substring(phoneType_len + charge_phone_len + startTime_len,
              phoneType_len + charge_phone_len + startTime_len + endTime_len).trim();
      String up_stream = str.substring(phoneType_len + charge_phone_len + startTime_len + endTime_len,
              phoneType_len + charge_phone_len + startTime_len + endTime_len + up_stream_len).trim();
      String down_stream = str.substring(phoneType_len + charge_phone_len + startTime_len + endTime_len + up_stream_len,
              phoneType_len + charge_phone_len + startTime_len + endTime_len + up_stream_len + down_stream_len).trim();
      String IMSI = str.substring(phoneType_len + charge_phone_len + startTime_len + endTime_len + up_stream_len + down_stream_len,
              phoneType_len + charge_phone_len + startTime_len + endTime_len + up_stream_len + down_stream_len + IMSI_len).trim();
      String IMEI = str.substring(phoneType_len + charge_phone_len + startTime_len + endTime_len + up_stream_len + down_stream_len + IMSI_len,
              phoneType_len + charge_phone_len + startTime_len + endTime_len + up_stream_len + down_stream_len + IMSI_len + IMEI_len).trim();
      String LAC = str.substring(phoneType_len + charge_phone_len + startTime_len + endTime_len + up_stream_len + down_stream_len + IMSI_len + IMEI_len,
              phoneType_len + charge_phone_len + startTime_len + endTime_len + up_stream_len + down_stream_len + IMSI_len + IMEI_len + LAC_len).trim();
      String CELL_ID = str.substring(phoneType_len + charge_phone_len + startTime_len + endTime_len + up_stream_len + down_stream_len + IMSI_len + IMEI_len + LAC_len,
              phoneType_len + charge_phone_len + startTime_len + endTime_len + up_stream_len + down_stream_len + IMSI_len + IMEI_len + LAC_len + CELL_ID_len).trim();
      String SGSN_IP = str.substring(phoneType_len + charge_phone_len + startTime_len + endTime_len + up_stream_len + down_stream_len + IMSI_len + IMEI_len + LAC_len + CELL_ID_len,
              phoneType_len + charge_phone_len + startTime_len + endTime_len + up_stream_len + down_stream_len + IMSI_len + IMEI_len + LAC_len + CELL_ID_len + SGSN_IP_len).trim();
      String GGSN_IP = str.substring(phoneType_len + charge_phone_len + startTime_len + endTime_len + up_stream_len + down_stream_len + IMSI_len + IMEI_len + LAC_len + CELL_ID_len + SGSN_IP_len,
              phoneType_len + charge_phone_len + startTime_len + endTime_len + up_stream_len + down_stream_len + IMSI_len + IMEI_len + LAC_len + CELL_ID_len + SGSN_IP_len + GGSN_IP_len).trim();
      String PLMN = str.substring(phoneType_len + charge_phone_len + startTime_len + endTime_len + up_stream_len + down_stream_len + IMSI_len + IMEI_len + LAC_len + CELL_ID_len + SGSN_IP_len + SGSN_IP_len,
              phoneType_len + charge_phone_len + startTime_len + endTime_len + up_stream_len + down_stream_len + IMSI_len + IMEI_len + LAC_len + CELL_ID_len + SGSN_IP_len + GGSN_IP_len + PLMN_len).trim();
      StringBuilder sb = new StringBuilder();
      sb.append(phoneType).append("|").append(charge_phone).append("|").append(startTime).append("|").append(endTime).append("|")
              .append(up_stream).append("|").append(down_stream).append("|").append(IMSI).append("|").append(IMEI).append("|").append(LAC).append("|")
              .append(CELL_ID).append("|").append(SGSN_IP).append("|").append(GGSN_IP).append("|").append(PLMN);
      return new byte[][]{sb.toString().getBytes()};
    }
  }
}
