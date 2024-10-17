/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.cache.processor;

import static java.nio.charset.StandardCharsets.UTF_8;

import api.ClientFactory;
import api.config.ConfigKey;
import api.consumer.NormalConsumer;
import api.producer.NormalProducer;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.cache.CacheOperation;
import com.datastrato.gravitino.cache.CacheService;
import com.datastrato.gravitino.cipher.CipherUtils;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.json.JsonUtils;
import com.google.common.base.Preconditions;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocketMQCacheAsyncProcessor implements CacheAsyncProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(RocketMQCacheAsyncProcessor.class);
  private final String topicName;
  private NormalProducer producer;
  private NormalConsumer consumer;
  private CacheService cacheService;

  public RocketMQCacheAsyncProcessor(Config config, CacheService cacheService) {
    String encryptedAK = config.get(Configs.ROCKETMQ_ASYNC_PROCESSOR_AK);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(encryptedAK), "The RocketMQ Async Processor AK is not set.");
    String ak = CipherUtils.decryptStringWithoutCompress(encryptedAK);

    String encryptedSK = config.get(Configs.ROCKETMQ_ASYNC_PROCESSOR_SK);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(encryptedSK), "The RocketMQ Async Processor SK is not set.");
    String sk = CipherUtils.decryptStringWithoutCompress(encryptedSK);

    String address = config.get(Configs.ROCKETMQ_ASYNC_PROCESSOR_ADDRESS);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(address), "The RocketMQ Async Processor Address is not set.");

    String topicGroup = config.get(Configs.ROCKETMQ_ASYNC_PROCESSOR_GROUP);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(topicGroup), "The RocketMQ Async Processor Group is not set.");

    this.topicName = config.get(Configs.ROCKETMQ_ASYNC_PROCESSOR_TOPIC);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(topicName), "The RocketMQ Async Processor Topic Name is not set.");

    Properties producerProperties = new Properties();
    producerProperties.setProperty(ConfigKey.PRODUCER_GROUP, topicGroup + "_producer");
    producerProperties.setProperty(ConfigKey.ACCESS_KEY, ak);
    producerProperties.setProperty(ConfigKey.SECRET_KEY, sk);
    producerProperties.setProperty(ConfigKey.NAME_SERVER_ADDR, address);
    producerProperties.setProperty(ConfigKey.ENABLE_MSG_TRACE, "true");
    createAndStartProducer(producerProperties);

    Properties consumerProperties = new Properties();
    consumerProperties.setProperty(ConfigKey.CONSUMER_GROUP, topicGroup + "_consumer");
    consumerProperties.setProperty(ConfigKey.ACCESS_KEY, ak);
    consumerProperties.setProperty(ConfigKey.SECRET_KEY, sk);
    consumerProperties.setProperty(ConfigKey.NAME_SERVER_ADDR, address);
    consumerProperties.setProperty(ConfigKey.ENABLE_MSG_TRACE, "true");
    createAndStartConsumer(consumerProperties);
    this.cacheService = cacheService;
  }

  private void createAndStartProducer(Properties properties) {
    this.producer = ClientFactory.createNormalProducer(properties);
    try {
      producer.start();
      LOG.info("RocketMQ Async Processor Producer started.");
    } catch (MQClientException e) {
      throw new GravitinoRuntimeException(
          e, "Exception occurs when starting the RocketMQ Async Processor Producer.");
    }
  }

  private void createAndStartConsumer(Properties properties) {
    this.consumer =
        ClientFactory.createNormalConsumer(
            properties,
            (msgs, consumeConcurrentlyContext) -> {
              LOG.info("{} Receive New Messages: {}", Thread.currentThread().getName(), msgs);
              for (MessageExt messageExt : msgs) {
                try {
                  String msg = new String(messageExt.getBody(), UTF_8);
                  AsyncCacheEvent event =
                      JsonUtils.anyFieldMapper().readValue(msg, AsyncCacheEvent.class);
                  switch (event.operation()) {
                    case DELETE:
                      cacheService.delete(event.type(), event.identifier());
                      break;
                    case DELETE_RECURSIVE:
                      cacheService.deleteRecursively(event.type(), event.identifier());
                      break;
                    default:
                      throw new GravitinoRuntimeException(
                          "Unsupported operation: " + event.operation());
                  }
                } catch (Exception e) {
                  LOG.error("Exception occurs when processing the message: {}.", messageExt, e);
                  return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
              }
              return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });

    try {
      consumer.subscribe(topicName, "*");
      consumer.start();
      LOG.info("RocketMQ Async Processor Consumer started.");
    } catch (MQClientException e) {
      throw new GravitinoRuntimeException(
          e, "Exception occurs when starting the RocketMQ Async Processor Consumer.");
    }
  }

  @Override
  public void process(CacheOperation operation, Entity.EntityType type, NameIdentifier identifier) {
    AsyncCacheEvent event = new AsyncCacheEvent(operation, type, identifier);
    try {
      String msg = JsonUtils.anyFieldMapper().writeValueAsString(event);
      Message message =
          new Message(this.topicName, operation.name(), identifier.toString(), msg.getBytes(UTF_8));
      SendResult sendResult = producer.send(message);
      LOG.info(
          "Send message to rocketmq success, sendResult: {}, sendMessage: {}", sendResult, msg);
    } catch (Exception e) {
      LOG.error("Failed to send event: {} to the topic", event, e);
    }
  }

  @Override
  public synchronized void close() {
    if (producer != null) {
      producer.shutdown();
      producer = null;
    }
    if (consumer != null) {
      consumer.shutdown();
      consumer = null;
    }
  }
}
