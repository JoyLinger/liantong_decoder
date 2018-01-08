package io.transwarp.test;

import io.transwarp.kafka.SingleProducer;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by zaish on 2016-10-23.
 */
public class a {
  private static Logger logger = Logger.getLogger(a.class);

  public a() {
    doShutDownWork();
  }

  private void doShutDownWork() {
    Runtime run = Runtime.getRuntime();
    run.addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.out.println(123);
      }
    });
  }

  public static void main(String[] args) throws IOException {
//        new a();
//        System.out.println("aaa");

    InputStream is = SingleProducer.class.getClassLoader().getResourceAsStream("topic.properties");
    Properties p = new Properties();
    p.load(is);
    System.out.println(p.getProperty("00_00"));
    logger.debug("aaaaaaaa");
//
//        String sourceDir1="C:\\Users\\zaish\\Desktop\\lt\\program\\test\\test.tar.gz";
//        File file=new File(sourceDir1);
//        byte[] rowkey= Bytes.toBytes("-1000117067");
//        System.out.println(rowkey);
  }
}
