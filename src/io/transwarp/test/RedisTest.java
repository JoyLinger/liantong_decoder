package io.transwarp.test;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by zaish on 2016-12-12.
 */
public class RedisTest {
  public static void main(String[] args) {
//       Jedis jedis=new Jedis("10.1.100.221",6379);
//       jedis.auth("foobared");
//       System.out.println(jedis.hvals("lac_info"));
//       System.out.println(jedis.hmget("lac_info:1819_64601","province_name","city_name","longitude_build","latitude_build"));
//       System.out.println(jedis.hmget("lac_info:1819_64601","longitude_build","latitude_build","city_name").get(0));
//       System.out.println(jedis.hmget("lac_info:1819_64601","city_name").get(0));
//       System.out.println(jedis.get("appinfo:-1000117067"));
//       jedis.close();
    for (int i = 0; i < 1000; i++) {
      Jedis jedis = new Jedis("10.1.100.221", 6379);
      jedis.auth("foobared");
      if (i % 1000 == 0) {
        System.out.println(i);
      }
    }
  }
}
