package io.transwarp.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Created by zaish on 2016-10-23.
 */
public class KafkaPartitioner implements Partitioner {

  public KafkaPartitioner(VerifiableProperties props) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public int partition(Object key, int numPartitions) {
    // TODO Auto-generated method stub
    return Math.abs(key.hashCode()) % numPartitions;
  }
}
