package io.transwarp.collect;

import io.transwarp.kafka.SingleProducer;
import kafka.producer.KeyedMessage;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

/**
 * Created by root on 3/22/17.
 * Format XL data.
 */
public class FormatXLData {

  private static kafka.javaapi.producer.Producer<String, String> producer = SingleProducer.getInstance();
  private static Logger LOGGER = Logger.getLogger(FormatXLData.class);
  private static Properties p;
  //    private static Properties p_check;
  private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static Properties paramProp;

  static {
    paramProp = new Properties();
    InputStream pis = FormatXLData.class.getClassLoader().getResourceAsStream("param.properties");
    try {
      paramProp.load(pis);
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  private static final String ErrorFlag = paramProp.getProperty("ErrorFlag");
  private static final String RightFlag = paramProp.getProperty("RightFlag");

  private static final int THREAD_NUM = Integer.parseInt(paramProp.getProperty("THREAD_NUM"));
  private static final ExecutorService es = Executors.newFixedThreadPool(THREAD_NUM);

  private FormatXLData() {
    InputStream is = FormatXLData.class.getClassLoader().getResourceAsStream("topic.properties");
//        InputStream is_check = SingleProducer.class.getClassLoader().getResourceAsStream("kafka_check.properties");
    p = new Properties();
//        p_check = new Properties();
    try {
      p.load(is);
//            p_check.load(is_check);
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
    }
    doShutDownWork();
  }

  //释放kafka资源
  private void doShutDownWork() {
    Runtime run = Runtime.getRuntime();
    run.addShutdownHook(new Thread() {
      @Override
      public void run() {
        producer.close();
        LOGGER.info("producer资源释放成功");
      }
    });
  }

  public static void main(String[] args) {
    if (args.length < 1 || args[0] == null || args[0].equals("")) {
      LOGGER.warn("路径参数缺失");
      return;
    }
    FormatXLData fxd = new FormatXLData();
    for (String filePath : args) {
      if (!new File(filePath.substring(0, filePath.lastIndexOf("/"))).exists()) {
        LOGGER.warn("File parent directory doesn't exist.");
        continue;
      }
      es.execute(new FormatXLData.MyThread(filePath, fxd));
    }
    es.shutdown();
  }


  private static class MyThread extends Thread {

    String fp;
    FormatXLData fxd;
    Long start;
    Long end;

    private MyThread(String filePath, FormatXLData fxd) {
      this.fp = filePath;
      this.fxd = fxd;
    }

    @Override
    public void run() {
      this.start = System.currentTimeMillis();
      this.fxd.run(this.fp);
      this.end = System.currentTimeMillis();
      LOGGER.info(this.fp + "处理完成,用时" + (this.end - this.start) + "ms");
    }
  }

  /*
  执行方法：输入tar.gz路径，处理里面每行数据
  完成工作：
  1.每行数据发到kafka
  2.处理后文件改后缀
  */
  private void run(String filePath) {
    long lineNum = 0;
    File file = new File(filePath);
    if (!file.exists()) {
      LOGGER.warn("File \'" + filePath + "\' doesn't exist.");
      return;
    }
    if (file.isDirectory()) {
      LOGGER.warn("\'" + filePath + "\' is a directory.");
      return;
    }
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(FileUtils.readFileToByteArray(file));
      Map<String, String[]> map = extractTarGzFile(bais, filePath);
      for (String tarArchiveEntry : map.keySet()) {
        String[] lines = map.get(tarArchiveEntry);
        if (lines == null) {
          continue;
        }
        lineNum += lines.length;
//                LOGGER.info("lines[0]:" + lines[0]);
        sendMessages(lines, tarArchiveEntry, filePath);
      }
      sendErrorMessages("file@@" + filePath + "@@" + sdf.format(new Date()) + "@@" + RightFlag + "@@" + lineNum);
      moveFile(filePath, ".complete");
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      sendErrorMessages("file@@" + filePath + "@@" + sdf.format(new Date()) + "@@" + ErrorFlag + "@@" + lineNum);
      moveFile(filePath, ".error");
    }
  }

  /*
  输入：tar.gz的ByteArrayInputStream
  输出：包含每行数据的字符串数组
   */
  private static Map<String, String[]> extractTarGzFile(ByteArrayInputStream is, String filePath) throws Exception {
    Map<String, String[]> map = new HashMap<>();
    GZIPInputStream gis = new GZIPInputStream(new BufferedInputStream(is));
    ArchiveInputStream ais = new ArchiveStreamFactory().createArchiveInputStream("tar", gis);
    TarArchiveEntry entry;
    int count = 0;
    //循环处理压缩包中的项目
    while ((entry = (TarArchiveEntry) ais.getNextEntry()) != null) {
      count++;
      LOGGER.info("Entry name: " + entry.getName());
      LOGGER.info("count = " + count);
      // 不是目录时才需要进行处理
      if (!entry.getName().endsWith("/")) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int bytesRead;
        while ((bytesRead = ais.read(buffer)) != -1) {
          baos.write(buffer, 0, bytesRead);
        }
        //  By now, just handle file in type 'txt' or 'dat'.
        if (entry.getName().endsWith(".txt") || entry.getName().endsWith(".dat")) {
          // 处理包含int数值的文件
          String[] messages = baos.toString().contains("\r") ? baos.toString().split("\r\n") : baos.toString().split("\n");
//                    String[] messages=baos.toString().split("\r\n");
          map.put(entry.getName(), messages);
//                    LOGGER.info("messages[0]: " + messages[0]);
        } else {
          LOGGER.error("Entry \'" + entry.getName() + "\' in tar file \'" + filePath + "\' is invalid");
//                    map.put(entry.getName(),null);
        }
//                if (entry.getName().endsWith("tar.gz")&&!entry.getName().contains(".txt")) {
////               处理压缩包
//                    return extractTarGzFile(new ByteArrayInputStream(baos.toByteArray()));
//                } else {
//                    // 处理包含int数值的文件
//                    String[] messages=baos.toString().split("\r\n");
//                    LOGGER.info("messages[0]: " + messages[0]);
//                    return messages;
//                }
      }
    }
    //压缩包处理完毕
    ais.close();
    gis.close();
    is.close();
    return map;
  }

  /*
  消息发送到kafka
   */
  private void sendMessages(String[] messages, String tarArchive, String path) {
    String topicName = "";
//        int lineNum = 0;
    for (String line : messages) {
//            lineNum++;
      StringBuilder topic_id = new StringBuilder();
      String[] words = line.split("\\|");

      if (words.length > 3) {
        switch (words[1]) {
          case "31":
            // S1MME
            switch (words[2]) {
              case "41":
                topic_id.append("3_41");
                break;
              case "42":
                topic_id.append("3_42");
                break;
              case "43":
                topic_id.append("3_43");
                break;
              case "21":
                topic_id.append("3_21");
                break;
              case "22":
                topic_id.append("3_22");
                break;
              case "24":
                topic_id.append("3_24");
                break;
              case "25":
                topic_id.append("3_25");
                break;
              case "26":
                topic_id.append("3_26");
                break;
              case "27":
                topic_id.append("3_27");
                break;
              case "28":
                topic_id.append("3_28");
                break;
              case "29":
                topic_id.append("3_29");
                break;
              case "33":
                topic_id.append("3_33");
                break;
              case "5":
                topic_id.append("3_5");
                break;
              case "6":
                topic_id.append("3_6");
                break;
              case "50":
                topic_id.append("3_50");
                break;
              default:
                topic_id.append("NULL");
                break;
            }
            break;
          // S11
          case "32":
            topic_id.append("2_32");
            break;
          // SGs
          case "33":
            topic_id.append("2_33");
            break;
          // S6a
          case "34":
            topic_id.append("2_34");
            break;
          // S10
          case "38":
            topic_id.append("2_38");
            break;
          /**
           // IuCS
           case "12":
           switch (words.length) {
           // XL_VOICE_CALL_TABLE
           case 125:
           topic_id.append("IuCS_125");
           break;
           default:
           topic_id.append("TBD");
           break;
           }
           break;
           // IuPS
           case "23":
           switch (words.length) {
           // xl_attach_require
           case 65:
           topic_id.append("IuPS_65");
           break;
           default:
           topic_id.append("TBD");
           break;
           }
           break;
           */
          default:
            topic_id.append("TBD");
            break;
        }
      } else {
        topic_id.append("wrong");
        LOGGER.info("Line length : " + words.length);
      }
      topicName = p.getProperty(topic_id.toString());
//            System.out.println("topic: " + topicName);
//            continue;
      if (topicName != null && !"".equals(topicName)) {
        producer.send(new KeyedMessage<>(topicName, UUID.randomUUID().toString(), line));
      } else {
        sendErrorMessages("line@@" + path + "/" + tarArchive + "/" + line + "@@" + sdf.format(new Date()) + "@@" + ErrorFlag);
//                LOGGER.error("No matched topic for data at line " + lineNum + ", tar file is '" + path + "', tar archive entry is '" + tarArchive + "'");
      }
    }
    LOGGER.info("Topic name  : " + topicName);
    LOGGER.info("Line number : " + messages.length);
  }

  /*
  消息发送到kafka
   */
  private void sendErrorMessages(String messages) {
    String topicName = p.getProperty("11_11");
//        LOGGER.info("Record monitor topic name  : " + topicName);
    producer.send(new KeyedMessage<>(topicName, UUID.randomUUID().toString(), messages));
  }

  /*
  输入文件全名称，改为complete后缀
   */
  private void moveFile(String sourceFile, String suffix) {
    String destFile = sourceFile + suffix;
    String[] cmds = {"/bin/sh", "-c", "mv " + sourceFile + " " + destFile};
    LOGGER.info(cmds[0] + cmds[1] + cmds[2]);
    Process pro;
    try {
      pro = Runtime.getRuntime().exec(cmds);
      pro.waitFor();
      InputStream in = pro.getInputStream();
      BufferedReader read = new BufferedReader(new InputStreamReader(in));
      String line;
      while ((line = read.readLine()) != null) {
        LOGGER.info(line);
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      //发送异常
      sendErrorMessages("file@@" + sourceFile + "@@" + sdf.format(new Date()) + "@@" + 11);
    }
  }
}
