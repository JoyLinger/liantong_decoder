package io.transwarp.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by zaish on 2016-9-12.
 */
public class Test {
  public static void main(String[] args) throws IOException, InterruptedException {
    String str = "6|32|34|460015400695808|867875021182321|8618605300780|460|01|2016-04-26 04:18:29.8217790|2016-04-26 04:18:29.8309930|0|0|000000000004dd21e6a8645b2ead01f9|34|1461615509821|1461615509830|0|16||10.177.121.110||116.79.229.138|116.79.200.8|2123|2123|2527266368|4125647549|2|5|1|6|3|1166937093|4125647549|5|1|6|3|1166937093|4125647549";
    StringBuilder result = new StringBuilder();
    String[] array = str.split("\\|");
    //32--S11接口话单  38--S10接口话单
    if (array[1].equals("32") || array[2].equals("38")) {
      StringBuilder bearer_info = new StringBuilder();
      int a = 28;
      for (int i = 0; i < Integer.parseInt(array[27]); i++) {
        if (i == Integer.parseInt(array[27]) - 1) {
          for (int j = 0; j < 6; j++) {
            if (j == 5) {
              bearer_info.append(array[a]);
              a++;
            } else {
              bearer_info.append(array[a]).append(",");
              a++;
            }
          }
        } else {
          for (int j = 0; j < 6; j++) {
            if (j == 5) {
              bearer_info.append(array[a]);
              a++;
            } else {
              bearer_info.append(array[a]).append(",");
              a++;
            }
          }
          bearer_info.append("@");
        }
      }
      for (int i = 0; i < 28; i++) {
        result.append(array[i]).append("|");
      }
      result.append(bearer_info);
    }
    System.out.println(result.toString());
  }
}
