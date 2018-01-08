package io.transwarp.test;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Created by root on 8/2/17.
 */
public class MapMemoryBuffer {
  public static void main(String[] args) throws Exception {
//        ungzipFile();

    /**
     *
     (1)
     Entry name: user_http.txt
     count = 1
     Entry content line number: 283995
     [Plan A] extractTarGzFile() takes time: 55219 ms
     [Plan A] Classify list takes time: 54488 ms
     [Plan A] takes time: 111215 ms
     Read 64191
     [Plan B] unTargzFile() + readByJavaNio() list takes time: 11804 ms
     [Plan B] Classify list takes time: 13 ms
     [Plan B] takes time: 11817 ms

     (2)
     Entry name: user_http.txt
     count = 1
     Entry content line number: 283995
     [Plan A] extractTarGzFile() takes time: 53939 ms
     [Plan A] Classify list takes time: 39794 ms
     [Plan A] takes time: 96278 ms
     Read 64191
     [Plan B] unTargzFile() + readByJavaNio() list takes time: 34803 ms
     [Plan B] Classify list takes time: 1 ms
     [Plan B] takes time: 34804 ms

     (3)
     Entry name: user_http.txt
     count = 1
     Entry content line number: 283995
     [Plan A] extractTarGzFile() takes time: 43894 ms
     [Plan A] Classify list takes time: 17482 ms
     [Plan A] takes time: 67016 ms
     Read 64191
     [Plan B] unTargzFile() + readByJavaNio() list takes time: 14144 ms
     [Plan B] Classify list takes time: 20 ms
     [Plan B] takes time: 14165 ms

     (4)
     Entry name: user_http.txt
     count = 1
     Entry content line number: 283995
     [Plan A] extractTarGzFile() takes time: 21028 ms
     [Plan A] Classify list takes time: 32923 ms
     [Plan A] takes time: 60957 ms
     Read 64191
     [Plan B] unTargzFile() + readByJavaNio() list takes time: 34217 ms
     [Plan B] Classify list takes time: 0 ms
     [Plan B] takes time: 34217 ms

     (5)
     [Plan A] extractTarGzFile() takes time: 94469 ms
     [Plan A] Classify list takes time: 100586 ms
     [Plan A] takes time: 195501 ms
     Read 64191
     [Plan B] unTargzFile() + readByJavaNio() list takes time: 44033 ms
     [Plan B] Classify list takes time: 127 ms
     [Plan B] takes time: 44160 ms

     (6)
     [Plan A] extractTarGzFile() takes time: 127710 ms
     [Plan A] Classify list takes time: 36546 ms
     [Plan A] takes time: 173374 ms
     Read 64191
     [Plan B] unTargzFile() + readByJavaNio() list takes time: 26013 ms
     [Plan B] Classify list takes time: 84 ms
     [Plan B] takes time: 26097 ms

     (7)
     [Plan A] extractTarGzFile() takes time: 85200 ms
     [Plan A] Classify list takes time: 148227 ms
     [Plan A] takes time: 241639 ms
     Read 64191
     [Plan B] unTargzFile() + readByJavaNio() list takes time: 39503 ms
     [Plan B] Classify list takes time: 0 ms
     [Plan B] takes time: 39503 ms

     (8)

     */
//        String fp = "/root/IdeaProjects/liantong_decoder/tar.gz/general_data1.tar.gz";

    String fp = "/root/IdeaProjects/liantong_decoder/tar.gz/user_http.tar.gz";

    long sta = System.currentTimeMillis();
    ByteArrayInputStream bais = new ByteArrayInputStream(FileUtils.readFileToByteArray(new File(fp)));

    long st1 = System.currentTimeMillis();
    Map<String, String[]> map = extractTarGzFile(bais, fp);
    long et1 = System.currentTimeMillis();
    System.out.println("[Plan A] extractTarGzFile() takes time: " + (et1 - st1) + " ms");

    for (String tarArchiveEntry : map.keySet()) {
      String[] lines = map.get(tarArchiveEntry);
      if (lines == null) {
        continue;
      }
      long st2 = System.currentTimeMillis();
      sendMessages(lines, tarArchiveEntry, fp);
      long et2 = System.currentTimeMillis();
      System.out.println("[Plan A] Classify list takes time: " + (et2 - st2) + " ms");
    }
    long eta = System.currentTimeMillis();
    System.out.println("[Plan A] takes time: " + (eta - sta) + " ms");

    /**
     *  unTargzFile() takes time: 14205 ms
     */
    long b_sta = System.currentTimeMillis();
    long b_st1 = System.currentTimeMillis();
    GZip.unTargzFile(fp, "/root/IdeaProjects/liantong_decoder/tar.gz/");
    String[] res = readByJavaNio("/root/IdeaProjects/liantong_decoder/tar.gz/general_data.dat");
    long b_et1 = System.currentTimeMillis();
    System.out.println("[Plan B] unTargzFile() + readByJavaNio() list takes time: " + (b_et1 - b_st1) + " ms");

    long b_st2 = System.currentTimeMillis();
    sendMessagesGood(res, "", fp);
    long b_et2 = System.currentTimeMillis();
    System.out.println("[Plan B] Classify list takes time: " + (b_et2 - b_st2) + " ms");

    long b_eta = System.currentTimeMillis();
    System.out.println("[Plan B] takes time: " + (b_eta - b_sta) + " ms");
//        System.out.println(res.length);
//        System.out.println(res[res.length - 1]);
  }

  public static String[] readByJavaNio(String txtFilePath) throws Exception {
//        byte[] bbb = new byte[1024];
    FileInputStream fis = new FileInputStream(txtFilePath);
//        FileOutputStream fos = new FileOutputStream("/root/IdeaProjects/liantong_decoder/tar.gz/user_http.copy1");
    FileChannel fc = fis.getChannel();
    long timeStar = System.currentTimeMillis();// µõ½µ±ǰµÄ±¼ä
    StringBuilder sb = new StringBuilder();
    ByteBuffer byteBuf = ByteBuffer.allocate((int) (fc.size()));
    int n = fc.read(byteBuf);
    while (n != -1 && n != 0) {
      System.out.println("Read " + n);
      byteBuf.flip();
      while (byteBuf.hasRemaining()) {
        sb.append((char) byteBuf.get());
      }
      n = fc.read(byteBuf);
    }
//        MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
//        System.out.println(fc.size()/1024);
    long timeEnd = System.currentTimeMillis();// µõ½µ±ǰµÄ±¼ä
//        System.out.println("txtFile read time :" + (timeEnd - timeStar) + "ms");
//        System.out.println("bbb length: " + bbb.length);
//        System.out.println(new String(bbb).substring(100));
    String[] messages = sb.toString().split("\r\n");
//        timeStar = System.currentTimeMillis();
//        fos.write(bbb);//2.дÈ
    //mbb.flip();
//        timeEnd = System.currentTimeMillis();
//        System.out.println("Write time :" + (timeEnd - timeStar) + "ms");
//        fos.flush();
    fc.close();
    fis.close();
    return messages;
  }

  public static void ungzipFile() throws Exception {
    FileInputStream fis = null;
    GZIPInputStream gis = null;
    FileOutputStream fos = null;
    try {
      long timeStar = System.currentTimeMillis();
      fis = new FileInputStream("/root/IdeaProjects/liantong_decoder/tar.gz/user_http.tar.gz");
      gis = new GZIPInputStream(fis);
      fos = new FileOutputStream("/root/IdeaProjects/liantong_decoder/tar.gz/user_http.copy1");
      byte[] buf = new byte[1024];
      int num;
      while ((num = gis.read(buf, 0, 1024)) != -1) {
        fos.write(buf);
      }
      long timeEnd = System.currentTimeMillis();
      System.out.println("I/O time: " + (timeEnd - timeStar) + "ms");
      fis.close();
      gis.close();
      fos.close();
    } catch (Exception e) {
      fis.close();
      gis.close();
      fos.close();
      e.printStackTrace();
    }
  }

  private static Map<String, String[]> extractTarGzFile(ByteArrayInputStream is, String filePath) throws Exception {
    Map<String, String[]> map = new HashMap<>();
    GZIPInputStream gis = new GZIPInputStream(new BufferedInputStream(is));
    ArchiveInputStream ais = new ArchiveStreamFactory().createArchiveInputStream("tar", gis);
    TarArchiveEntry entry;
    int count = 0;
    //循环处理压缩包中的项目
    while ((entry = (TarArchiveEntry) ais.getNextEntry()) != null) {
      count++;
      System.out.println("Entry name: " + entry.getName());
      System.out.println("count = " + count);
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
          String[] messages = baos.toString().split("\r\n");
          map.put(entry.getName(), messages);
          System.out.println("Entry content line number: " + messages.length);
        } else {
          System.out.println("Entry \'" + entry.getName() + "\' in tar file \'" + filePath + "\' is invalid");
//                    map.put(entry.getName(),null);
        }
//                if (entry.getName().endsWith("tar.gz")&&!entry.getName().contains(".txt")) {
////               处理压缩包
//                    return extractTarGzFile(new ByteArrayInputStream(baos.toByteArray()));
//                } else {
//                    // 处理包含int数值的文件
//                    String[] messages=baos.toString().split("\r\n");
//                    System.out.println("messages[0]: " + messages[0]);
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
  public static void sendMessages(String[] messages, String tarArchive, String path) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String topicName = "";
//        int lineNum = 0;
    for (String line : messages) {
//            lineNum++;
      StringBuilder topic_id = new StringBuilder();
      long st1 = System.currentTimeMillis();
      String[] words = line.split("\\|");
      long et1 = System.currentTimeMillis();
//            System.out.println("[Plan A] split() takes time: " + (et1 - st1) + " ms");
      if (words[0].length() > 2 || words[0].length() == 0) {
        // GENERAL
        topic_id.append("00_00");
      } else {
        // check starttime
//                try{
//                    sdf.parse(words[1]);
//                    sdf.parse(words[2]);
//                }catch(Exception e){
////                    sendErrorMessages("line@@" + path + "/" + tarArchive + "/" + line + "@@" + sdf.format(new Date()) + "@@" + ErrorFlag);
//                    continue;
//                }
        topic_id.append(words[0]);
      }
    }
  }

  /*
  消息发送到kafka
   */
  public static void sendMessagesGood(String[] messages, String tarArchive, String path) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String topicName = "";
//        int lineNum = 0;
    for (String line : messages) {
//            lineNum++;
      StringBuilder topic_id = new StringBuilder();
      long st1 = System.currentTimeMillis();
//            String[] words = line.split("\\|");
      String key = getCharByRank(line, "|", 3);
      long et1 = System.currentTimeMillis();
//            System.out.println("[Plan B] substring() takes time: " + (et1 - st1) + " ms");
//            if ((words[0].length() > 2 || words[0].length() == 0) && words.length == 52) {
      if (key.length() > 2 || key.length() == 0) {
        // GENERAL
        topic_id.append("00_00");
      } else {
        // check starttime
//                try{
////                    sdf.parse(words[1]);
////                    sdf.parse(words[2]);
//                    String st = line.substring(line.indexOf("|") + 1, line.indexOf("|", line.indexOf("|") + 1));
//                    String et = line.substring(line.indexOf("|", line.indexOf("|") + 1) + 1, line.indexOf("|", line.indexOf("|", line.indexOf("|") + 1) + 1));
//                    sdf.parse(st);
//                    sdf.parse(et);
//                }catch(Exception e){
////                    sendErrorMessages("line@@" + path + "/" + tarArchive + "/" + line + "@@" + sdf.format(new Date()) + "@@" + ErrorFlag);
//                    continue;
//                }
//                topic_id.append(word[0]);
        topic_id.append(key);
      }
    }
  }

  private static String getCharByRank(String sourceStr, String spliter, int rank) {
    int index = 0;
    int count = 0;
    int from = 0;
    while ((index = sourceStr.indexOf(spliter, index)) != -1) {
      index += spliter.length();
      count++;
      if (count == rank) {
        return sourceStr.substring(from, index - 1);
      } else if (count + 1 == rank) {
        from = index;
      }
    }
    return null;
  }
}
