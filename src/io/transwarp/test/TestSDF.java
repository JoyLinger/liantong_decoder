package io.transwarp.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by root on 7/23/17.
 */
public class TestSDF {
  private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static String[] dateStr = {"2017-07-23 17:55:43,763", "20170723 17:55:43", "2017-07-23", "7F5E", "2017-07-23 17:55:43", "20170723", "2017-05-03 00:00:28.9799600"};

  public static void main(String[] args) {
    for (String ds : dateStr) {
      try {
        System.out.println(sdf.parse(ds));
      } catch (ParseException e) {
        System.out.println("Error: " + ds + " can't parse");
        continue;
      }
    }
  }
}
