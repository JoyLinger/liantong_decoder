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
 * Created by root on 2/15/17.
 * Handle USER data file(tar.gz or gz)
 * tips:
 * 1. By now, just handle file with type 'txt' or 'dat' inside the tar.gz file.
 */
public class FormatXDData {

  private static kafka.javaapi.producer.Producer<String, String> producer = SingleProducer.getInstance();
  private static Logger LOGGER = Logger.getLogger(FormatXDData.class);
  private static Properties p;
  //    private static Properties p_check;
  private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static Properties paramProp;

  static {
    paramProp = new Properties();
    InputStream pis = FormatXDData.class.getClassLoader().getResourceAsStream("param.properties");
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

  private FormatXDData() {
    InputStream is = FormatXDData.class.getClassLoader().getResourceAsStream("topic.properties");
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
    FormatXDData fyd = new FormatXDData();
    for (String filePath : args) {
      if (!new File(filePath.substring(0, filePath.lastIndexOf("/"))).exists()) {
        LOGGER.warn("File parent directory doesn't exist.");
        continue;
      }
      es.execute(new MyThread(filePath, fyd));
    }
    es.shutdown();
  }


  private static class MyThread extends Thread {

    String fp;
    FormatXDData fyd;
    Long start;
    Long end;

    private MyThread(String filePath, FormatXDData fyd) {
      this.fp = filePath;
      this.fyd = fyd;
    }

    @Override
    public void run() {
      this.start = System.currentTimeMillis();
      this.fyd.run(this.fp);
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
      String topicName = classify(file);
      ByteArrayInputStream bais = new ByteArrayInputStream(FileUtils.readFileToByteArray(file));
      String[] lines = extractGzFile(bais, filePath);
      if (lines != null) {
        lineNum += lines.length;
        sendMessages(lines, "", filePath, topicName);
      }
      sendErrorMessages("file@@" + filePath + "@@" + sdf.format(new Date()) + "@@" + RightFlag + "@@" + lineNum);
      moveFile(filePath, ".complete");
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      sendErrorMessages("file@@" + filePath + "@@" + sdf.format(new Date()) + "@@" + ErrorFlag + "@@" + lineNum);
      moveFile(filePath, ".error");
    }
  }

  private static String classify(File f) {
    if (f.getName().endsWith(".01001.gz")) {
      return p.getProperty("01001");
    } else if (f.getName().endsWith(".01002.gz")) {
      return p.getProperty("01002");
    } else if (f.getName().endsWith(".01003.gz")) {
      return p.getProperty("01003");
    } else {
      return "wrong";
    }
  }

  /*
  输入：tar.gz的ByteArrayInputStream
  输出：包含每行数据的字符串数组
   */
  private static String[] extractGzFile(ByteArrayInputStream is, String filePath) throws Exception {
    GZIPInputStream gis = new GZIPInputStream(new BufferedInputStream(is));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buffer = new byte[4096];
    int bytesRead;
    while ((bytesRead = gis.read(buffer)) != -1) {
      baos.write(buffer, 0, bytesRead);
    }
    // 处理包含int数值的文件
    String[] messages = baos.toString().contains("\r") ? baos.toString().split("\r\n") : baos.toString().split("\n");
    gis.close();
    is.close();
    return messages;
  }


  /*
  消息发送到kafka
   */
  private void sendMessages(String[] messages, String tarArchive, String path, String topicName) {
    if (topicName == null || topicName.equals("")) {
//        int lineNum = 0;
      for (String line : messages) {
//            lineNum++;
        StringBuilder topic_id = new StringBuilder();
        int len = line.length();
        if (len == 141 || len == 101 || len == 183) {
          topic_id.append(len);
        } else {
          topic_id.append("wrong");
        }
        topicName = p.getProperty(topic_id.toString());
        if (topicName != null && !"".equals(topicName)) {
          producer.send(new KeyedMessage<>(topicName, UUID.randomUUID().toString(), line));
        } else {
          sendErrorMessages("line@@" + path + "/" + tarArchive + "/" + line + "@@" + sdf.format(new Date()) + "@@" + ErrorFlag);
//                LOGGER.error("No matched topic for data at line " + lineNum + ", tar file is '" + path + "', tar archive entry is '" + tarArchive + "'");
        }
      }
    } else {
      List<KeyedMessage<String, String>> messageList = new ArrayList<>();
      for (String line : messages) {
        if (!"wrong".equals(topicName)) {
          messageList.add(new KeyedMessage<>(topicName, UUID.randomUUID().toString(), line));
        } else {
          sendErrorMessages("line@@" + path + "/" + tarArchive + "/" + line + "@@" + sdf.format(new Date()) + "@@" + ErrorFlag);
//                LOGGER.error("No matched topic for data at line " + lineNum + ", tar file is '" + path + "', tar archive entry is '" + tarArchive + "'");
        }
      }
      producer.send(messageList);
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
