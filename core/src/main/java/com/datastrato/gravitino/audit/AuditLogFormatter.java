/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.audit;

import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.MetalakeChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.listener.api.event.AlterCatalogEvent;
import com.datastrato.gravitino.listener.api.event.AlterCatalogFailureEvent;
import com.datastrato.gravitino.listener.api.event.AlterFilesetEvent;
import com.datastrato.gravitino.listener.api.event.AlterFilesetFailureEvent;
import com.datastrato.gravitino.listener.api.event.AlterMetalakeEvent;
import com.datastrato.gravitino.listener.api.event.AlterMetalakeFailureEvent;
import com.datastrato.gravitino.listener.api.event.AlterSchemaEvent;
import com.datastrato.gravitino.listener.api.event.AlterSchemaFailureEvent;
import com.datastrato.gravitino.listener.api.event.AlterTableEvent;
import com.datastrato.gravitino.listener.api.event.AlterTableFailureEvent;
import com.datastrato.gravitino.listener.api.event.AlterTopicEvent;
import com.datastrato.gravitino.listener.api.event.AlterTopicFailureEvent;
import com.datastrato.gravitino.listener.api.event.CatalogEvent;
import com.datastrato.gravitino.listener.api.event.CatalogFailureEvent;
import com.datastrato.gravitino.listener.api.event.CreateCatalogEvent;
import com.datastrato.gravitino.listener.api.event.CreateCatalogFailureEvent;
import com.datastrato.gravitino.listener.api.event.CreateFilesetEvent;
import com.datastrato.gravitino.listener.api.event.CreateFilesetFailureEvent;
import com.datastrato.gravitino.listener.api.event.CreateMetalakeEvent;
import com.datastrato.gravitino.listener.api.event.CreateMetalakeFailureEvent;
import com.datastrato.gravitino.listener.api.event.CreateSchemaEvent;
import com.datastrato.gravitino.listener.api.event.CreateSchemaFailureEvent;
import com.datastrato.gravitino.listener.api.event.CreateTableEvent;
import com.datastrato.gravitino.listener.api.event.CreateTableFailureEvent;
import com.datastrato.gravitino.listener.api.event.CreateTopicEvent;
import com.datastrato.gravitino.listener.api.event.CreateTopicFailureEvent;
import com.datastrato.gravitino.listener.api.event.Event;
import com.datastrato.gravitino.listener.api.event.FailureEvent;
import com.datastrato.gravitino.listener.api.event.FilesetEvent;
import com.datastrato.gravitino.listener.api.event.FilesetFailureEvent;
import com.datastrato.gravitino.listener.api.event.ListCatalogEvent;
import com.datastrato.gravitino.listener.api.event.ListCatalogFailureEvent;
import com.datastrato.gravitino.listener.api.event.ListFilesetEvent;
import com.datastrato.gravitino.listener.api.event.ListFilesetFailureEvent;
import com.datastrato.gravitino.listener.api.event.ListMetalakeEvent;
import com.datastrato.gravitino.listener.api.event.ListMetalakeFailureEvent;
import com.datastrato.gravitino.listener.api.event.ListSchemaEvent;
import com.datastrato.gravitino.listener.api.event.ListSchemaFailureEvent;
import com.datastrato.gravitino.listener.api.event.ListTableEvent;
import com.datastrato.gravitino.listener.api.event.ListTableFailureEvent;
import com.datastrato.gravitino.listener.api.event.ListTopicEvent;
import com.datastrato.gravitino.listener.api.event.ListTopicFailureEvent;
import com.datastrato.gravitino.listener.api.event.LoadCatalogEvent;
import com.datastrato.gravitino.listener.api.event.LoadCatalogFailureEvent;
import com.datastrato.gravitino.listener.api.event.LoadFilesetEvent;
import com.datastrato.gravitino.listener.api.event.LoadFilesetFailureEvent;
import com.datastrato.gravitino.listener.api.event.LoadMetalakeEvent;
import com.datastrato.gravitino.listener.api.event.LoadMetalakeFailureEvent;
import com.datastrato.gravitino.listener.api.event.LoadSchemaEvent;
import com.datastrato.gravitino.listener.api.event.LoadSchemaFailureEvent;
import com.datastrato.gravitino.listener.api.event.LoadTableEvent;
import com.datastrato.gravitino.listener.api.event.LoadTableFailureEvent;
import com.datastrato.gravitino.listener.api.event.LoadTopicEvent;
import com.datastrato.gravitino.listener.api.event.LoadTopicFailureEvent;
import com.datastrato.gravitino.listener.api.event.MetalakeEvent;
import com.datastrato.gravitino.listener.api.event.MetalakeFailureEvent;
import com.datastrato.gravitino.listener.api.event.SchemaEvent;
import com.datastrato.gravitino.listener.api.event.SchemaFailureEvent;
import com.datastrato.gravitino.listener.api.event.TableEvent;
import com.datastrato.gravitino.listener.api.event.TableFailureEvent;
import com.datastrato.gravitino.listener.api.event.TopicEvent;
import com.datastrato.gravitino.listener.api.event.TopicFailureEvent;
import com.datastrato.gravitino.listener.api.info.CatalogInfo;
import com.datastrato.gravitino.listener.api.info.FilesetInfo;
import com.datastrato.gravitino.listener.api.info.MetalakeInfo;
import com.datastrato.gravitino.listener.api.info.SchemaInfo;
import com.datastrato.gravitino.listener.api.info.TableInfo;
import com.datastrato.gravitino.listener.api.info.TopicInfo;
import com.datastrato.gravitino.messaging.TopicChange;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.TableChange;
import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Formatter for audit log. */
public class AuditLogFormatter implements Formatter<AuditLog> {
  private Logger LOG = LoggerFactory.getLogger(AuditLogFormatter.class);

  @Override
  public AuditLog format(Event event) {
    AuditLog auditLog = fromEvent(event);
    return auditLog;
  }

  public AuditLog fromEvent(Event event) {
    if (event instanceof FailureEvent) {
      return fromFailEvent((FailureEvent) event);
    } else {
      return fromSuccessEvent(event);
    }
  }

  /**
   * Convert the event to AuditLog.
   *
   * @param event
   * @return
   */
  private AuditLog fromSuccessEvent(Event event) {
    Long timestamp = event.eventTime();
    AuditLog.Action action = AuditLog.Action.from(event);
    AuditLog.ObjectType objectType = null;
    AuditLog.Request request = null;
    if (event instanceof MetalakeEvent) {
      objectType = AuditLog.ObjectType.METALAKE;
      request = parseEvent((MetalakeEvent) event);
    } else if (event instanceof CatalogEvent) {
      objectType = AuditLog.ObjectType.CATALOG;
      request = parseEvent((CatalogEvent) event);
    } else if (event instanceof SchemaEvent) {
      objectType = AuditLog.ObjectType.SCHEMA;
      request = parseEvent((SchemaEvent) event);
    } else if (event instanceof TableEvent) {
      objectType = AuditLog.ObjectType.TABLE;
      request = parseEvent((TableEvent) event);
    } else if (event instanceof TopicEvent) {
      objectType = AuditLog.ObjectType.TOPIC;
      request = parseEvent((TopicEvent) event);
    } else if (event instanceof FilesetEvent) {
      objectType = AuditLog.ObjectType.FILESET;
      request = parseEvent((FilesetEvent) event);
    } else {
      objectType = AuditLog.ObjectType.UNKNOWN;
      request = parseUnsupportedEvent(event);
    }
    AuditLog.Response response = AuditLog.Response.ofSuccess();
    String identifier = parseIdentifier(event.identifier());
    return new AuditLog(
        event.user(),
        action,
        objectType,
        identifier,
        request,
        response,
        event.getClass().getSimpleName(),
        timestamp);
  }

  /**
   * Convert the event to AuditLog.
   *
   * @param event
   * @return
   */
  private AuditLog fromFailEvent(FailureEvent event) {
    Long timestamp = event.eventTime();
    AuditLog.Action action = AuditLog.Action.from(event);
    AuditLog.ObjectType objectType = null;
    AuditLog.Request request = null;
    if (event instanceof MetalakeFailureEvent) {
      objectType = AuditLog.ObjectType.METALAKE;
      request = parseEvent((MetalakeFailureEvent) event);
    } else if (event instanceof CatalogFailureEvent) {
      objectType = AuditLog.ObjectType.CATALOG;
      request = parseEvent((CatalogFailureEvent) event);
    } else if (event instanceof SchemaFailureEvent) {
      objectType = AuditLog.ObjectType.SCHEMA;
      request = parseEvent((SchemaFailureEvent) event);
    } else if (event instanceof TableFailureEvent) {
      objectType = AuditLog.ObjectType.TABLE;
      request = parseEvent((TableFailureEvent) event);
    } else if (event instanceof TopicFailureEvent) {
      objectType = AuditLog.ObjectType.TOPIC;
      request = parseEvent((TopicFailureEvent) event);
    } else if (event instanceof FilesetFailureEvent) {
      objectType = AuditLog.ObjectType.FILESET;
      request = parseEvent((FilesetFailureEvent) event);
    } else {
      objectType = AuditLog.ObjectType.UNKNOWN;
      request = parseUnsupportedEvent(event);
    }
    AuditLog.Response response = AuditLog.Response.ofFailure(event.exception().getMessage());
    return new AuditLog(
        event.user(),
        action,
        objectType,
        parseIdentifier(event.identifier()),
        request,
        response,
        event.getClass().getSimpleName(),
        timestamp);
  }

  /**
   * Convert the identifier to a string, which removes the namespace if it exists. for example, if
   * the identifier is "metalakeName.catalogName.databaseName.tableName", the result will be
   * "catalogName.databaseName.tableName".
   *
   * @param identifier
   * @return
   */
  private String parseIdentifier(NameIdentifier identifier) {
    if (identifier == null) {
      return null;
    }
    if (identifier.hasNamespace()) {
      Namespace namespace = identifier.namespace();
      String metalake = namespace.levels()[0];
      return StringUtils.substring(identifier.toString(), metalake.length() + 1);
    } else {
      return identifier.toString();
    }
  }

  private AuditLog.Request<MetalakeInfo, MetalakeChange> parseEvent(MetalakeEvent event) {
    if (event instanceof CreateMetalakeEvent) {
      CreateMetalakeEvent createMetalakeEvent = (CreateMetalakeEvent) event;
      return new AuditLog.Request<>(createMetalakeEvent.createdMetalakeInfo());
    } else if (event instanceof AlterMetalakeEvent) {
      AlterMetalakeEvent alterMetalakeEvent = (AlterMetalakeEvent) event;
      return new AuditLog.Request<>(
          alterMetalakeEvent.updatedMetalakeInfo(),
          parseChange(alterMetalakeEvent.metalakeChanges()));
    } else if (event instanceof ListMetalakeEvent || event instanceof LoadMetalakeEvent) {
      return new AuditLog.Request<>();
    } else {
      return null;
    }
  }

  private AuditLog.Request<MetalakeInfo, MetalakeChange> parseEvent(MetalakeFailureEvent event) {
    if (event instanceof CreateMetalakeFailureEvent) {
      CreateMetalakeFailureEvent createMetalakeEvent = (CreateMetalakeFailureEvent) event;
      return new AuditLog.Request<>(createMetalakeEvent.createMetalakeRequest());
    } else if (event instanceof AlterMetalakeFailureEvent) {
      AlterMetalakeFailureEvent alterMetalakeEvent = (AlterMetalakeFailureEvent) event;
      return new AuditLog.Request<>(null, parseChange(alterMetalakeEvent.metalakeChanges()));
    } else if (event instanceof ListMetalakeFailureEvent
        || event instanceof LoadMetalakeFailureEvent) {
      return new AuditLog.Request<>();
    } else {
      return null;
    }
  }

  private AuditLog.Request<CatalogInfo, CatalogChange> parseEvent(CatalogEvent event) {
    if (event instanceof CreateCatalogEvent) {
      CreateCatalogEvent createCatalogEvent = (CreateCatalogEvent) event;
      CatalogInfo catalogInfo = createCatalogEvent.createdCatalogInfo();
      return new AuditLog.Request(catalogInfo);
    } else if (event instanceof AlterCatalogEvent) {
      AlterCatalogEvent catalogEvent = (AlterCatalogEvent) event;
      CatalogChange[] catalogChange = catalogEvent.catalogChanges();
      return new AuditLog.Request<>(catalogEvent.updatedCatalogInfo(), parseChange(catalogChange));
    } else if (event instanceof ListCatalogEvent || event instanceof LoadCatalogEvent) {
      return new AuditLog.Request();
    } else {
      return null;
    }
  }

  private AuditLog.Request<CatalogInfo, CatalogChange> parseEvent(CatalogFailureEvent event) {
    if (event instanceof CreateCatalogFailureEvent) {
      CreateCatalogFailureEvent createCatalogEvent = (CreateCatalogFailureEvent) event;
      CatalogInfo catalogInfo = createCatalogEvent.createCatalogRequest();
      return new AuditLog.Request<>(catalogInfo);
    } else if (event instanceof AlterCatalogFailureEvent) {
      AlterCatalogFailureEvent alterCatalogEvent = (AlterCatalogFailureEvent) event;
      return new AuditLog.Request<>(null, parseChange(alterCatalogEvent.catalogChanges()));
    } else if (event instanceof ListCatalogFailureEvent
        || event instanceof LoadCatalogFailureEvent) {
      return new AuditLog.Request<>();
    } else {
      return null;
    }
  }

  private AuditLog.Request<SchemaInfo, SchemaChange> parseEvent(SchemaEvent event) {
    if (event instanceof CreateSchemaEvent) {
      CreateSchemaEvent createSchemaEvent = (CreateSchemaEvent) event;
      SchemaInfo schemaInfo = createSchemaEvent.createdSchemaInfo();
      return new AuditLog.Request<SchemaInfo, SchemaChange>(schemaInfo);
    } else if (event instanceof AlterSchemaEvent) {
      AlterSchemaEvent alterSchemaEvent = (AlterSchemaEvent) event;
      return new AuditLog.Request<SchemaInfo, SchemaChange>(
          alterSchemaEvent.updatedSchemaInfo(), parseChange(alterSchemaEvent.schemaChanges()));
    } else if (event instanceof ListSchemaEvent || event instanceof LoadSchemaEvent) {
      return new AuditLog.Request<SchemaInfo, SchemaChange>();
    } else {
      return null;
    }
  }

  private AuditLog.Request<SchemaInfo, SchemaChange> parseEvent(SchemaFailureEvent event) {
    if (event instanceof CreateSchemaFailureEvent) {
      CreateSchemaFailureEvent createSchemaEvent = (CreateSchemaFailureEvent) event;
      SchemaInfo schemaInfo = createSchemaEvent.createSchemaRequest();
      return new AuditLog.Request<SchemaInfo, SchemaChange>(schemaInfo);
    } else if (event instanceof AlterSchemaFailureEvent) {
      AlterSchemaFailureEvent alterSchemaEvent = (AlterSchemaFailureEvent) event;
      return new AuditLog.Request<SchemaInfo, SchemaChange>(
          null, parseChange(alterSchemaEvent.schemaChanges()));
    } else if (event instanceof ListSchemaFailureEvent || event instanceof LoadSchemaFailureEvent) {
      return new AuditLog.Request<SchemaInfo, SchemaChange>();
    } else {
      return null;
    }
  }

  private AuditLog.Request<TableInfo, TableChange> parseEvent(TableEvent event) {
    if (event instanceof CreateTableEvent) {
      CreateTableEvent createTableEvent = (CreateTableEvent) event;
      TableInfo tableInfo = createTableEvent.createdTableInfo();
      return new AuditLog.Request<TableInfo, TableChange>(tableInfo);
    } else if (event instanceof AlterTableEvent) {
      AlterTableEvent alterTableEvent = (AlterTableEvent) event;
      return new AuditLog.Request<TableInfo, TableChange>(
          alterTableEvent.updatedTableInfo(), parseChange(alterTableEvent.tableChanges()));
    } else if (event instanceof ListTableEvent || event instanceof LoadTableEvent) {
      return new AuditLog.Request<>();
    } else {
      return null;
    }
  }

  private AuditLog.Request<TableInfo, TableChange> parseEvent(TableFailureEvent event) {
    if (event instanceof CreateTableFailureEvent) {
      CreateTableFailureEvent createTableEvent = (CreateTableFailureEvent) event;
      TableInfo tableInfo = createTableEvent.createTableRequest();
      return new AuditLog.Request<TableInfo, TableChange>(tableInfo);
    } else if (event instanceof AlterTableFailureEvent) {
      AlterTableFailureEvent alterTableEvent = (AlterTableFailureEvent) event;
      return new AuditLog.Request<TableInfo, TableChange>(
          null, parseChange(alterTableEvent.tableChanges()));
    } else if (event instanceof ListTableFailureEvent || event instanceof LoadTableFailureEvent) {
      return new AuditLog.Request<TableInfo, TableChange>();
    } else {
      return null;
    }
  }

  private AuditLog.Request<FilesetInfo, FilesetChange> parseEvent(FilesetEvent event) {
    if (event instanceof CreateFilesetEvent) {
      CreateFilesetEvent createFilesetEvent = (CreateFilesetEvent) event;
      FilesetInfo filesetInfo = createFilesetEvent.createdFilesetInfo();
      return new AuditLog.Request<FilesetInfo, FilesetChange>(filesetInfo);
    } else if (event instanceof AlterFilesetEvent) {
      AlterFilesetEvent alterFilesetEvent = (AlterFilesetEvent) event;
      return new AuditLog.Request<FilesetInfo, FilesetChange>(
          alterFilesetEvent.updatedFilesetInfo(), parseChange(alterFilesetEvent.filesetChanges()));
    } else if (event instanceof ListFilesetEvent || event instanceof LoadFilesetEvent) {
      return new AuditLog.Request<>();
    } else {
      return null;
    }
  }

  private AuditLog.Request<FilesetInfo, FilesetChange> parseEvent(FilesetFailureEvent event) {
    if (event instanceof CreateFilesetFailureEvent) {
      CreateFilesetFailureEvent createFilesetEvent = (CreateFilesetFailureEvent) event;
      FilesetInfo filesetInfo = createFilesetEvent.createFilesetRequest();
      return new AuditLog.Request<FilesetInfo, FilesetChange>(filesetInfo);
    } else if (event instanceof AlterFilesetFailureEvent) {
      AlterFilesetFailureEvent alterFilesetEvent = (AlterFilesetFailureEvent) event;
      return new AuditLog.Request<FilesetInfo, FilesetChange>(
          null, parseChange(alterFilesetEvent.filesetChanges()));
    } else if (event instanceof ListFilesetFailureEvent
        || event instanceof LoadFilesetFailureEvent) {
      return new AuditLog.Request<FilesetInfo, FilesetChange>();
    } else {
      return null;
    }
  }

  private AuditLog.Request<TopicInfo, TopicChange> parseEvent(TopicEvent event) {
    if (event instanceof CreateTopicEvent) {
      CreateTopicEvent createTopicEvent = (CreateTopicEvent) event;
      TopicInfo topicInfo = createTopicEvent.createdTopicInfo();
      return new AuditLog.Request<TopicInfo, TopicChange>(topicInfo);
    } else if (event instanceof AlterTopicEvent) {
      AlterTopicEvent alterTopicEvent = (AlterTopicEvent) event;
      return new AuditLog.Request<TopicInfo, TopicChange>(
          alterTopicEvent.updatedTopicInfo(), parseChange(alterTopicEvent.topicChanges()));
    } else if (event instanceof ListTopicEvent || event instanceof LoadTopicEvent) {
      return new AuditLog.Request<>();
    } else {
      return null;
    }
  }

  private AuditLog.Request<TopicInfo, TopicChange> parseEvent(TopicFailureEvent event) {
    if (event instanceof CreateTopicFailureEvent) {
      CreateTopicFailureEvent createTopicEvent = (CreateTopicFailureEvent) event;
      TopicInfo topicInfo = createTopicEvent.createTopicRequest();
      return new AuditLog.Request<TopicInfo, TopicChange>(topicInfo);
    } else if (event instanceof AlterTopicFailureEvent) {
      AlterTopicFailureEvent alterTopicEvent = (AlterTopicFailureEvent) event;
      return new AuditLog.Request<TopicInfo, TopicChange>(
          null, parseChange(alterTopicEvent.topicChanges()));
    } else if (event instanceof ListTopicFailureEvent || event instanceof LoadTopicFailureEvent) {
      return new AuditLog.Request<TopicInfo, TopicChange>();
    } else {
      return null;
    }
  }

  private AuditLog.ChangeInfo[] parseChange(MetalakeChange[] changes) {
    return Arrays.stream(changes)
        .map(
            change ->
                new AuditLog.ChangeInfo<MetalakeChange>(change.getClass().getSimpleName(), change))
        .toArray(AuditLog.ChangeInfo[]::new);
  }

  private AuditLog.ChangeInfo[] parseChange(CatalogChange[] changes) {
    return Arrays.stream(changes)
        .map(
            change ->
                new AuditLog.ChangeInfo<CatalogChange>(change.getClass().getSimpleName(), change))
        .toArray(AuditLog.ChangeInfo[]::new);
  }

  private AuditLog.ChangeInfo[] parseChange(SchemaChange[] changes) {
    return Arrays.stream(changes)
        .map(
            change ->
                new AuditLog.ChangeInfo<SchemaChange>(change.getClass().getSimpleName(), change))
        .toArray(AuditLog.ChangeInfo[]::new);
  }

  private AuditLog.ChangeInfo<FilesetChange>[] parseChange(FilesetChange[] changes) {
    return Arrays.stream(changes)
        .map(
            change ->
                new AuditLog.ChangeInfo<FilesetChange>(change.getClass().getSimpleName(), change))
        .toArray(AuditLog.ChangeInfo[]::new);
  }

  private AuditLog.ChangeInfo<TableChange>[] parseChange(TableChange[] changes) {
    return Arrays.stream(changes)
        .map(
            change ->
                new AuditLog.ChangeInfo<TableChange>(change.getClass().getSimpleName(), change))
        .toArray(AuditLog.ChangeInfo[]::new);
  }

  private AuditLog.ChangeInfo<TopicChange>[] parseChange(TopicChange[] changes) {
    return Arrays.stream(changes)
        .map(
            change ->
                new AuditLog.ChangeInfo<TopicChange>(change.getClass().getSimpleName(), change))
        .toArray(AuditLog.ChangeInfo[]::new);
  }

  private AuditLog.Request<Void, Void> parseUnsupportedEvent(Event event) {
    LOG.warn("Unsupported event type: {}, detail:{} ", event.getClass().getName(), event);
    return new AuditLog.Request<>();
  }
}
