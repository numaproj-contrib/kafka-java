package io.numaproj.kafka.producer;

import io.numaproj.kafka.config.UserConfig;
import io.numaproj.numaflow.sinker.Sinker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.DisposableBean;

public abstract class BaseKafkaSinker<T> extends Sinker implements DisposableBean {
  protected final UserConfig userConfig;
  protected final KafkaProducer<String, T> producer;

  protected BaseKafkaSinker(UserConfig userConfig, KafkaProducer<String, T> producer) {
    this.userConfig = userConfig;
    this.producer = producer;
  }
}
