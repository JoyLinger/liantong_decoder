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
public class FormatUserData {
  private static Logger LOGGER = Logger.getLogger(FormatUserData.class);

  private static kafka.javaapi.producer.Producer<String, String> producer = SingleProducer.getInstance();

  private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private static Properties p;
  private static Properties paramProp;

  static {
    p = new Properties();
    paramProp = new Properties();
    InputStream is = FormatUserData.class.getClassLoader().getResourceAsStream("topic.properties");
    InputStream pis = FormatUserData.class.getClassLoader().getResourceAsStream("param.properties");
    try {
      p.load(is);
      paramProp.load(pis);
    } catch (IOException e) {
      LOGGER.error(e.getStackTrace());
    }
  }

  private static final String ErrorFlag = paramProp.getProperty("ErrorFlag");
  private static final String RightFlag = paramProp.getProperty("RightFlag");
  private static final String SUFFIX = paramProp.getProperty("SUFFIX");
  private static final String FIELD_SPLITER = paramProp.getProperty("FIELD_SPLITER");
  private static final String DOS_LINE_SPLITER = paramProp.getProperty("DOS_LINE_SPLITER");
  private static final String UNIX_LINE_SPLITER = paramProp.getProperty("UNIX_LINE_SPLITER");
  private static final int THREAD_NUM = Integer.parseInt(paramProp.getProperty("THREAD_NUM"));
  private static final ExecutorService es = Executors.newFixedThreadPool(THREAD_NUM);


  public FormatUserData() {
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
      LOGGER.error("路径参数缺失");
      return;
    }
    FormatUserData fud = new FormatUserData();
    for (String monitorDir : args) {
      File mdf = new File(monitorDir);
      if (!mdf.exists()) {
        LOGGER.info("The monitor directory " + monitorDir + " doesn't exist.");
        continue;
      }
      List<String> allFilePaths = getFilePaths(mdf);
      LOGGER.info("First allFilePaths size: " + allFilePaths.size());
      while (allFilePaths != null && allFilePaths.size() > 0) {
        for (String fp : allFilePaths) {
          moveFile(monitorDir + "/" + fp, ".processing");
          es.execute(new MyTask(monitorDir + "/" + fp + ".processing", fud));
        }
        allFilePaths = getFilePaths(mdf);
        while (allFilePaths == null || allFilePaths.size() == 0) {
          try {
            Thread.sleep(60000);
          } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
          }
          allFilePaths = getFilePaths(mdf);
        }
        System.out.println("New allFilePaths size: " + allFilePaths.size());
      }
    }
    es.shutdown();
  }

  private static List<String> getFilePaths(File f) {
    String[] files = f.list(new FilenameFilter() {
      String suffix = SUFFIX;

      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(suffix);
      }
    });
    LOGGER.info("List<String> getFilePaths(File f): okay");
    return files == null ? new ArrayList<String>() : Arrays.asList(files);
  }

  private static class MyTask implements Runnable {

    String fp;
    FormatUserData fud;
    Long start;
    Long end;

    private MyTask(String filePath, FormatUserData fud) {
      this.fp = filePath;
      this.fud = fud;
      LOGGER.info("MyTask(String filePath,FormatUserData fud): okay");
    }

    @Override
    public void run() {
      this.start = System.currentTimeMillis();
      this.fud.run(this.fp);
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
      LOGGER.info("File \'" + filePath + "\' doesn't exist.");
      return;
    }
    if (file.isDirectory()) {
      LOGGER.info("\'" + filePath + "\' is a directory.");
      return;
    }
    LOGGER.info("File \'" + filePath + "\' exist and is a file: okay");
    try {
      if (filePath.endsWith(".txt.processing")) {
        // txt
        FileInputStream fis = new FileInputStream(file);
//                InputStreamReader isr = new InputStreamReader(fis);
//                BufferedReader br = new BufferedReader(isr);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int bytesRead;
        while ((bytesRead = fis.read(buffer)) != -1) {
          baos.write(buffer, 0, bytesRead);
        }
        String[] lines = baos.toString().contains("\r") ? baos.toString().split(DOS_LINE_SPLITER) : baos.toString().split(UNIX_LINE_SPLITER);
        LOGGER.info("line num: " + lines.length);
        sendMessagesGood(lines, "", filePath);
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {
          LOGGER.error(e.getMessage(), e);
        }
      } else if (filePath.endsWith(".tar.gz.processing")) {
        ByteArrayInputStream bais = new ByteArrayInputStream(FileUtils.readFileToByteArray(file));
        long st1 = System.currentTimeMillis();
        Map<String, String[]> map = extractTarGzFile(bais, filePath);
        long et1 = System.currentTimeMillis();
        LOGGER.info("extractTarGzFile() takes time: " + (et1 - st1) + " ms");
        for (String tarArchiveEntry : map.keySet()) {
          String[] lines = map.get(tarArchiveEntry);
          if (lines == null) {
            continue;
          }
          lineNum += lines.length;
          st1 = System.currentTimeMillis();
          sendMessagesGood(lines, tarArchiveEntry, filePath);
          et1 = System.currentTimeMillis();
          LOGGER.info("sendMessagesGood() takes time: " + (et1 - st1) + " ms");
          try {
            Thread.sleep(10000);
          } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
          }
        }
      }
      LOGGER.info("file@@" + filePath + "@@" + sdf.format(new Date()) + "@@" + RightFlag + "@@" + lineNum);
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
//        int count = 0;
    //循环处理压缩包中的项目
    while ((entry = (TarArchiveEntry) ais.getNextEntry()) != null) {
//            count++;
//            LOGGER.info("Entry name: " + entry.getName());
//            LOGGER.info("count = " + count);
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
          String[] messages = baos.toString().contains("\r") ? baos.toString().split(DOS_LINE_SPLITER) : baos.toString().split(UNIX_LINE_SPLITER);
          map.put(entry.getName(), messages);
        } else {
          LOGGER.error("Entry \'" + entry.getName() + "\' in tar file \'" + filePath + "\' is invalid");
        }
      }
    }
    //压缩包处理完毕
    ais.close();
    gis.close();
    is.close();
    LOGGER.info("Map<String,String[]> extractTarGzFile(ByteArrayInputStream is, String filePath): okay");
    return map;
  }


  /*
  消息发送到kafka
   */
  private static void sendMessagesGood(String[] messages, String tarArchive, String path) {
    String topicName;
    for (String line : messages) {
//            LOGGER.info(line);
      StringBuilder topic_id = new StringBuilder();
      String key = getCharByRank(line, FIELD_SPLITER, 1);
      if (key.length() > 2 || key.length() == 0) {
        // GENERAL
        topic_id.append("00_00");
      } else {
        topic_id.append(key);
      }
      topicName = p.getProperty(topic_id.toString());
      if (topicName != null && !"".equals(topicName)) {
        producer.send(new KeyedMessage<>(topicName, UUID.randomUUID().toString(), line));
      } else {
        sendErrorMessages("line@@" + path + "/" + tarArchive + "/" + line + "@@" + sdf.format(new Date()) + "@@" + ErrorFlag);
      }
    }
//        LOGGER.info("Topic name  : " + topicName);
//        LOGGER.info("Line number : " + messages.length);
  }

  /*
  消息发送到kafka
   */
  private static void sendErrorMessages(String messages) {
    String topicName = p.getProperty("11_11");
    producer.send(new KeyedMessage<>(topicName, UUID.randomUUID().toString(), messages));
  }

  /*
  输入文件全名称，改为complete后缀
   */
  private static void moveFile(String sourceFile, String suffix) {
    String destFile = sourceFile + suffix;
    String[] cmds = {"/bin/sh", "-c", "mv " + sourceFile + " " + destFile};
    LOGGER.info(cmds[0] + cmds[1] + cmds[2]);
    Process pro;
    try {
      Runtime.getRuntime().exec(cmds);
      pro = Runtime.getRuntime().exec(cmds);
      pro.waitFor();
      InputStream in = pro.getInputStream();
      BufferedReader read = new BufferedReader(new InputStreamReader(in));
      String line;
      while ((line = read.readLine()) != null) {
        LOGGER.info("bash -c mv res: " + line);
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      //发送异常
      sendErrorMessages("file@@" + sourceFile + "@@" + sdf.format(new Date()) + "@@" + 11);
    } finally {
      LOGGER.info("void moveFile(String sourceFile, String suffix): okay");
    }
  }

  private static String getCharByRank(String sourceStr, String spliter, int rank) {
    int index = 0, count = 0, from = 0;
    while ((index = sourceStr.indexOf(spliter, index)) != -1) {
      index += spliter.length();
      count++;
      if (rank == count) {
        return sourceStr.substring(from, index - 1);
      } else if (rank - 1 == count) {
        from = index;
      }
    }
    return "";
  }
}
