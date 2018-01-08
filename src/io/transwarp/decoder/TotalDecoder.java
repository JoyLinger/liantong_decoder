package io.transwarp.decoder;

import io.transwarp.streaming.sql.api.decoder.ArrayDecoder;
import kafka.utils.VerifiableProperties;

/**
 * Created by zaish on 2016-10-10.
 */
public class TotalDecoder extends ArrayDecoder<byte[]> {
  public TotalDecoder(VerifiableProperties props) {
    super(props);
  }

  @Override
  public byte[][] arrayFromBytes(byte[] bytes) {
    String str = new String(bytes);
    StringBuilder result = new StringBuilder();
    String[] array = str.split("\\|");
    //32--S11接口话单  38--S10接口话单
    if (array[1].equals("32") || array[2].equals("38")) {
      for (int i = 0; i < 27; i++) {
        result.append(array[i]).append("|");
      }
      String bearer_info = this.resolveBearerInfo(27, array);
      result.append(bearer_info);
    } else if (array[1].equals("36") || array[2].equals("37")) {//36--S5接口话单  37--S8接口话单
      for (int i = 0; i < 29; i++) {
        result.append(array[i]).append("|");
      }
      String bearer_info = this.resolveBearerInfo(29, array);
      result.append(bearer_info);
    } else {
      result.append(str);
    }
    return new byte[][]{result.toString().getBytes()};
  }

  //解析bearer_info
  public String resolveBearerInfo(int index, String[] array) {
    StringBuilder bearer_info = new StringBuilder();
    int a = index + 1;
    int num = Integer.parseInt(array[index]);
    bearer_info.append(array[index]);
    for (int i = 0; i < num; i++) {
      //添加分隔符，如果第一个用|隔开
      if (i == 0) {
        bearer_info.append("|");
      } else {
        bearer_info.append("@");
      }
      //拼接一组bearer
      for (int j = 0; j < 6; j++) {
        if (j == 5) {
          bearer_info.append(array[a]);
          a++;
        } else {
          bearer_info.append(array[a]).append(",");
          a++;
        }
      }
    }
    return bearer_info.toString();
  }

  public static void main(String[] args) {
    String str = "6|32|34|460015400695808|867875021182321|8618605300780|460|01|2016-04-26 04:18:29.8217790|2016-04-26 04:18:29.8309930|0|0|000000000004dd21e6a8645b2ead01f9|34|1461615509821|1461615509830|0|16||10.177.121.110||116.79.229.138|116.79.200.8|2123|2123|2527266368|4125647549|2|5|1|6|3|1166937093|4125647549|5|1|6|3|1166937093|4125647549";
    String[] array = str.split("\\|");
    System.out.println(new TotalDecoder(null).resolveBearerInfo(27, array));
  }
}
