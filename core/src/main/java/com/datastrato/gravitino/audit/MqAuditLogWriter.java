/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.audit;

import static java.nio.charset.StandardCharsets.UTF_8;

import api.ClientFactory;
import api.config.ConfigKey;
import api.producer.NormalProducer;
import com.datastrato.gravitino.cipher.CipherUtils;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.json.JsonUtils;
import java.util.Map;
import java.util.Properties;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqAuditLogWriter extends AbstractAuditLogWriter {

  private static final Logger LOG = LoggerFactory.getLogger(MqAuditLogWriter.class);

  private NormalProducer producer;
  private String topic;

  public MqAuditLogWriter(Formatter formatter) {
    super(formatter);
  }

  static class Config {
    static final String TOPIC = "rocketmq.producer.topic";
    static final String ACCESS_KEY = "rocketmq.producer.access-key";
    static final String SECRET_KEY = "rocketmq.producer.secret-key";
    static final String ADDRESS = "rocketmq.producer.address";
    static final String ENABLE_MSG_TRACE = "rocketmq.producer.enable-msg-trace";
    static final String PRODUCER_GROUP = "rocketmq.producer.group";
  }

  @Override
  public void init(Map<String, String> config) {
    String ak = CipherUtils.decryptStringWithoutCompress(config.get(Config.ACCESS_KEY));
    String sk = CipherUtils.decryptStringWithoutCompress(config.get(Config.SECRET_KEY));
    String address = config.get(Config.ADDRESS);
    Properties rocketMqProperties = new Properties();
    rocketMqProperties.setProperty(ConfigKey.PRODUCER_GROUP, config.get(Config.PRODUCER_GROUP));
    rocketMqProperties.setProperty(ConfigKey.ACCESS_KEY, ak);
    rocketMqProperties.setProperty(ConfigKey.SECRET_KEY, sk);
    rocketMqProperties.setProperty(ConfigKey.NAME_SERVER_ADDR, address);
    rocketMqProperties.setProperty(
        ConfigKey.ENABLE_MSG_TRACE, config.get(Config.ENABLE_MSG_TRACE)); // /开启消息轨迹
    this.topic = config.get(Config.TOPIC);
    this.producer = ClientFactory.createNormalProducer(rocketMqProperties);
    try {
      this.producer.start();
    } catch (Exception e) {
      throw new GravitinoRuntimeException(e, "Failed to start rocketmq producer");
    }
  }

  @Override
  public void doWrite(Object event) {
    try {
      AuditLog auditLog = (AuditLog) event;
      AuditLog.TAG tag = AuditLog.TAG.from(auditLog.getAction());
      String msg = JsonUtils.eventFieldMapper().writeValueAsString(event);
      Message message =
          new Message(this.topic, tag.name(), auditLog.getIdentifier(), msg.getBytes(UTF_8));
      SendResult sendResult = producer.send(message);
      LOG.info("Send message to rocketmq success, sendResult: {}", sendResult);
    } catch (Exception e) {
      LOG.error("Failed to send message to rocketmq", e);
    }
  }

  @Override
  public void close() {
    producer.shutdown();
  }
}
