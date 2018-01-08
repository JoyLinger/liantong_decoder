package io.transwarp.collect;

import io.transwarp.kafka.SingleProducer;
import kafka.producer.KeyedMessage;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

/**
 * Created by zaish on 2016-10-18.
 */
public class XLSendData {
  private static kafka.javaapi.producer.Producer<String, String> producer = SingleProducer.getInstance();
  private static Logger logger = Logger.getLogger(XLSendData.class);
  private static Properties p;
  private static Properties p_check;

  private XLSendData() {
    InputStream is = SingleProducer.class.getClassLoader().getResourceAsStream("topic.properties");
    InputStream is_check = SingleProducer.class.getClassLoader().getResourceAsStream("kafka_check.properties");
    p = new Properties();
    p_check = new Properties();
    try {
      p.load(is);
      p_check.load(is_check);
    } catch (IOException e) {
      e.printStackTrace();
    }
    doShutDownWork();
  }

  /*
  执行方法：输入tar.gz路径，处理里面每行数据
  完成工作：
  1.每行数据发到kafka
  2.处理后文件改后缀
  */
  private void run(String sourcedir) {
    File file = new File(sourcedir);
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(FileUtils.readFileToByteArray(file));
      String[] lines = extractTarGzFile(bais);
      sendMessages(lines);
      moveFile(sourcedir, ".complete");
    } catch (Exception e) {
      e.printStackTrace();
      moveFile(sourcedir, ".error");
//            sendMessages(sourcedir);
    }
  }

  /*
  输入文件全名称，改为complete后缀
   */
  private void moveFile(String sourcedir, String suffix) {
    String destDir = sourcedir + suffix;
    String[] cmds = {"/bin/sh", "-c", "mv " + sourcedir + " " + destDir};
    System.out.println(cmds[0] + cmds[1] + cmds[2]);
    Process pro = null;
    try {
      pro = Runtime.getRuntime().exec(cmds);
      pro.waitFor();
      InputStream in = pro.getInputStream();
      BufferedReader read = new BufferedReader(new InputStreamReader(in));
      String line = null;
      while ((line = read.readLine()) != null) {
        System.out.println(line);
      }
    } catch (Exception e) {
      e.printStackTrace();
      //发送异常
      System.out.println("移动文件" + sourcedir + "失败");
    }
  }

  /*
  消息发送到kafka
   */
  private void sendMessages(String[] messages) {
    for (String line : messages) {
      String[] words = line.split("\\|");
      StringBuilder topic_id = new StringBuilder();
      //第一个字段长度大于1则认为是通用话单
      if (words[0].length() > 2) {
        if (words.length != Integer.parseInt(p.getProperty(words[0]))) {
//                    sendErrorMessages(p_check.getProperty("00_00")+"@@"+line+"@@"+);
        }
        topic_id.append("00_00");
      } else {
        topic_id.append(words[0]);
      }
      producer.send(new KeyedMessage<String, String>(p.getProperty(topic_id.toString()), UUID.randomUUID().toString(), line));
    }
  }

  //    /*
//    消息发送到kafka
//     */
//    private void sendErrorMessages(String messages){
//        for(String line:messages){
//            String[] words=line.split("\\|");
//            StringBuilder topic_id=new StringBuilder();
//            //第一个字段长度大于1则认为是通用话单
//            if(words[0].length()>2){
//                topic_id.append("00_00");
//            }else{
//                topic_id.append(words[1]).append("_").append(words[2]);
//            }
//            producer.send(new KeyedMessage<String, String>(p.getProperty(topic_id.toString()),UUID.randomUUID().toString(),line));
//        }
//    }
  //释放kafka资源
  private void doShutDownWork() {
    Runtime run = Runtime.getRuntime();
    run.addShutdownHook(new Thread() {
      @Override
      public void run() {
        producer.close();
        System.out.println("producer资源释放成功");
      }
    });
  }

  /*
  输入：tar.jz的ByteArrayInputStream
  输出：包含每行数据的字符串数组
   */
  private static String[] extractTarGzFile(ByteArrayInputStream is) throws Exception {
    GZIPInputStream gis = new GZIPInputStream(new BufferedInputStream(is));
    ArchiveInputStream ais = new ArchiveStreamFactory().createArchiveInputStream("tar", gis);
    TarArchiveEntry entry = null;
    //循环处理压缩包中的项目
    while ((entry = (TarArchiveEntry) ais.getNextEntry()) != null) {
      // 不是目录时才需要进行处理
      if (!entry.getName().endsWith("/")) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int bytesRead = -1;
        while ((bytesRead = ais.read(buffer)) != -1) {
          baos.write(buffer, 0, bytesRead);
        }
        if (entry.getName().endsWith("tar.gz") && !entry.getName().contains(".txt")) {
//               处理压缩包
          extractTarGzFile(new ByteArrayInputStream(
                  baos.toByteArray()));
        } else {
          // 处理包含int数值的文件
          String[] messages = baos.toString().split("\r\n");
          return messages;
        }
      }
    }
    //压缩包处理完毕
    ais.close();
    gis.close();
    is.close();
    //如没数据返回null
    return null;
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      logger.warn("路径参数缺失");
      return;
    }
    Long start = 0L;
    Long end = 0L;
    XLSendData sd = new XLSendData();
    for (String sourceDir : args) {
      start = System.currentTimeMillis();
      sd.run(sourceDir);
      end = System.currentTimeMillis();
      System.out.println(sourceDir + "处理完成,用时" + (end - start) + "ms");
    }
    ;
  }
}
