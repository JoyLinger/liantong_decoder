package io.transwarp.collect;

import io.transwarp.kafka.SingleProducer;
import kafka.producer.KeyedMessage;
//import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.ChunkedWriter;
import org.apache.log4j.Logger;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

/**
 * Created by zaish on 2016-10-18.
 * Handle USER data file(tar.gz or gz)
 */
public class UserSendDataBak {
  private static kafka.javaapi.producer.Producer<String, String> producer = SingleProducer.getInstance();
  private static Logger logger = Logger.getLogger(UserSendDataBak.class);
  private static Properties p;
  private static Properties p_check;
  private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static final String ErrorFlag = "0";
  private static final String RightFlag = "1";

  private static ExecutorService fixedThreadPool = Executors.newFixedThreadPool(20);
  private static String[] lines;
  private static boolean endOfFile = false;
  private static int lineNum = 0;

  private class Run implements Runnable {
    ByteArrayInputStream bais;
    int start, end;

    private Run(ByteArrayInputStream bais, int start, int end) {
      this.bais = bais;
      this.start = start;
      this.end = end;
    }

    @Override
    public void run() {
      try {
        UserSendDataBak.lines = extractTarGzFile(bais, start, end);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private UserSendDataBak() {
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
  private void run(String filePath, int flag, int start, int end) {
    File file = new File(filePath);
    try {
//            String[] lines = null;
      if (flag == 1) {
        ByteArrayInputStream bais = new ByteArrayInputStream(FileUtils.readFileToByteArray(file));
        fixedThreadPool.execute(new Run(bais, start, end));
//                lines = extractTarGzFile(bais);
      } else if (flag == 2) {
        ByteArrayInputStream bais = new ByteArrayInputStream(FileUtils.readFileToByteArray(file));
        lines = extractGzFile(bais);
      } else if (flag == 0) {
        System.out.println("Directory or already decoded, exit.");
        return;
      } else if (flag == -1) {
        System.out.println("Unknown file type, exit.");
        return;
      } else {
        return;
      }
      if (lines != null && lines.length > 0) {
        System.out.println("line[" + 0 + "] = " + lines[0]);
        sendMessages(lines);
//                sendErrorMessages("file@@"+filePath+"@@"+sdf.format(new Date())+"@@"+RightFlag);
        moveFile(filePath, ".complete");
      } else if (lines == null) {
        endOfFile = true;
      }
    } catch (Exception e) {
      e.printStackTrace();
      sendErrorMessages("file@@" + filePath + "@@" + sdf.format(new Date()) + "@@" + ErrorFlag);
      moveFile(filePath, ".error");
    }
  }

  /*
  输入文件全名称，改为complete后缀
   */
  private void moveFile(String sourcedir, String suffix) {
    String destDir = sourcedir + suffix;
    String[] cmds = {"/bin/sh", "-c", "mv " + sourcedir + " " + destDir};
    System.out.println(cmds[0] + cmds[1] + cmds[2]);
    Process pro;
    try {
      pro = Runtime.getRuntime().exec(cmds);
      pro.waitFor();
      InputStream in = pro.getInputStream();
      BufferedReader read = new BufferedReader(new InputStreamReader(in));
      String line;
      while ((line = read.readLine()) != null) {
        System.out.println(line);
      }
    } catch (Exception e) {
      e.printStackTrace();
      //发送异常
      sendErrorMessages("file@@" + sourcedir + "@@" + sdf.format(new Date()) + "@@" + 11);
    }
  }

  /*
  消息发送到kafka
   */
  private void sendMessages(String[] messages) {
    for (String line : messages) {
      String[] words = line.split("\\|");
      StringBuilder topic_id = new StringBuilder();
      //第一个字段长度大于2则认为是通用话单
      if (words[0].length() > 2) {
        // USER.GENERAL
        if (words.length != Integer.parseInt(p_check.getProperty("00_00"))) {
          sendErrorMessages(p_check.getProperty("00_00_NAME") + "@@" + line + "@@" + sdf.format(new Date()) + "@@" + ErrorFlag);
          continue;
        } else {
          topic_id.append("00_00");
        }
      } else {
        // USER.HTTP, DNS, RTSP, VOIP, P2P, MMS, EMAIL, FTP, OTHERS, IM, STREAMING
        if (words.length != Integer.parseInt(p_check.getProperty(words[0] + "_NAME"))) {
          sendErrorMessages(p_check.getProperty(words[0] + "_NAME") + "@@" + line + "@@" + sdf.format(new Date()) + "@@" + ErrorFlag);
          continue;
        } else {
          topic_id.append(words[0]);
        }
      }
      System.out.println("Topic: " + p.getProperty(topic_id.toString()));
      System.out.println("Data : " + line);
//            producer.send(new KeyedMessage<>(p.getProperty(topic_id.toString()),UUID.randomUUID().toString(),line));
    }
  }

  /*
  消息发送到kafka
   */
  private void sendErrorMessages(String messages) {
    String topic_id = p.getProperty("11_11");
    producer.send(new KeyedMessage<>(topic_id, UUID.randomUUID().toString(), messages));
  }

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
  输入：tar.gz的ByteArrayInputStream
  输出：包含每行数据的字符串数组
   */
  private static String[] extractTarGzFile(ByteArrayInputStream is, int start, int end) throws Exception {
    GZIPInputStream gis = new GZIPInputStream(new BufferedInputStream(is));
    ArchiveInputStream ais = new ArchiveStreamFactory().createArchiveInputStream("tar", gis);
    Reader r = new InputStreamReader(ais);
    BufferedReader br = new BufferedReader(r);
    TarArchiveEntry entry;
    //循环处理压缩包中的项目
    while ((entry = (TarArchiveEntry) ais.getNextEntry()) != null) {
      // 不是目录时才需要进行处理
      if (!entry.getName().endsWith("/")) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Writer w = new OutputStreamWriter(baos);
        BufferedWriter bw = new BufferedWriter(w);
//                byte[] buffer = new byte[4096];
//                int bytesRead;
        String line;
        lineNum = 0;
        while ((line = br.readLine()) != null) {
          lineNum++;
          if (lineNum < start) {
            continue;
          }
          System.out.println("Start write at line: " + lineNum);
          bw.write(line);
          bw.newLine();
          if (lineNum > end) {
            gis.close();
            ais.close();
            r.close();
            br.close();
            break;
          }
//                    System.out.println(baos.size() + "\t" + new String(baos.toByteArray()).split("\n").length);
        }
        if (entry.getName().endsWith("tar.gz") && !entry.getName().contains(".txt")) {
//               处理压缩包
          extractTarGzFile(new ByteArrayInputStream(
                  baos.toByteArray()), start, end);
        } else {
          // 处理包含int数值的文件
          return baos.toString().split("\r\n");
        }
      }
    }
    //压缩包处理完毕
    gis.close();
    ais.close();
    r.close();
    br.close();
    //如没数据返回null
    return null;
  }

  /*
  输入：gz的ByteArrayInputStream
  输出：包含每行数据的字符串数组
   */
  private static String[] extractGzFile(ByteArrayInputStream is) throws Exception {
    GZIPInputStream gis = new GZIPInputStream(new BufferedInputStream(is));
    Scanner sc = new Scanner(gis);
    List<String> lines = new ArrayList<>();
    while (sc.hasNextLine()) {
      lines.add(sc.nextLine());
    }
    //压缩包处理完毕
    gis.close();
    is.close();
    //如没数据返回null
    return (String[]) lines.toArray();
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      logger.warn("路径参数缺失");
      return;
    }
    Long start;
    Long end;
    UserSendDataBak sd = new UserSendDataBak();
    for (String sourceDir : args) {
      File dir = new File(sourceDir);
      String[] fileList = dir.list();
      if (fileList == null) {
        continue;
      }
      for (String fp : fileList) {
        start = System.currentTimeMillis();
        System.out.println("Start handle data file: " + sourceDir + "/" + fp);
        int flag = -1;
        if (new File(sourceDir + "/" + fp).isDirectory()) {
          // Directory
          flag = 0;
        } else if (fp.endsWith("tar.gz")) {
          // tar.gz file
          flag = 1;
        } else if (fp.endsWith(".gz")) {
          // .gz file
          flag = 2;
        } else if (fp.endsWith(".complete")) {
          // decoded file
          flag = 0;
        }
        int i = 0;
        while (true) {
          int startLine = i * 10000 + 1;
          int endLine = (i + 1) * 10000;
          sd.run(sourceDir + "/" + fp, flag, startLine, endLine);
          if (endOfFile) {
            break;
          }
          i++;
        }
        end = System.currentTimeMillis();
        System.out.println(sourceDir + "/" + fp + "处理完成,用时" + (end - start) + "ms");
      }
    }
  }
}
