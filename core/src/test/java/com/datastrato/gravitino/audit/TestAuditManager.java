/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.audit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.TestFileset;
import com.datastrato.gravitino.TestFilesetContext;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.file.BaseFilesetDataOperationCtx;
import com.datastrato.gravitino.file.ClientType;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetContext;
import com.datastrato.gravitino.file.FilesetDataOperation;
import com.datastrato.gravitino.file.FilesetDataOperationCtx;
import com.datastrato.gravitino.file.SourceEngineType;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.listener.EventBus;
import com.datastrato.gravitino.listener.EventListenerManager;
import com.datastrato.gravitino.listener.api.event.AlterTableEvent;
import com.datastrato.gravitino.listener.api.event.AlterTableFailureEvent;
import com.datastrato.gravitino.listener.api.event.CreateTableEvent;
import com.datastrato.gravitino.listener.api.event.CreateTableFailureEvent;
import com.datastrato.gravitino.listener.api.event.DropTableEvent;
import com.datastrato.gravitino.listener.api.event.DropTableFailureEvent;
import com.datastrato.gravitino.listener.api.event.Event;
import com.datastrato.gravitino.listener.api.event.GetFilesetContextEvent;
import com.datastrato.gravitino.listener.api.event.GetFilesetContextFailureEvent;
import com.datastrato.gravitino.listener.api.info.TableInfo;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.distributions.Strategy;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrders;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
import com.datastrato.gravitino.rel.types.Types;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestAuditManager {

  private CreateTableEvent createTableEvent;

  private AlterTableEvent alterTableEvent;

  private DropTableEvent dropTableEvent;

  private DummyEvent unknownEvent;

  private CreateTableFailureEvent createTableFailureEvent;

  private AlterTableFailureEvent alterTableFailureEvent;
  private DropTableFailureEvent dropTableFailureEvent;
  private GetFilesetContextEvent getFilesetContextEvent;

  private GetFilesetContextFailureEvent getFilesetContextFailureEvent;

  @BeforeAll
  public void init() {
    this.createTableEvent = mockCreateTableEvent();
    this.alterTableEvent = mockAlterTableEvent();
    this.dropTableEvent = mockDropTableEvent();
    this.createTableFailureEvent = mockCreateTableFailureEvent();
    this.dropTableFailureEvent = mockDropTableFailEvent();
    this.alterTableFailureEvent = mockAlterTableFailEvent();
    this.getFilesetContextEvent = mockGetFilesetContextEvent();
    this.getFilesetContextFailureEvent = mockGetFilesetContextFailureEvent();
    this.unknownEvent = mockUnknownEvent();
  }

  @Test
  public void testCreateTable() {
    AuditLogManager auditLogManager = mockAndDispatchEvent(createTableEvent);
    Assertions.assertInstanceOf(DummyAuditWriter.class, auditLogManager.getAuditLogWriter());
    Assertions.assertInstanceOf(
        DummyAuditFormatter.class,
        ((DummyAuditWriter) auditLogManager.getAuditLogWriter()).getFormatter());
    DummyAuditWriter dummyAuditWriter = (DummyAuditWriter) auditLogManager.getAuditLogWriter();
    Assertions.assertEquals(1, dummyAuditWriter.getAuditLogs().size());
    Assertions.assertInstanceOf(Map.class, dummyAuditWriter.getAuditLogs().get(0));
  }

  private Map<String, String> createManagerConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("enable", "true");
    properties.put("writer.class", "com.datastrato.gravitino.audit.DummyAuditWriter");
    properties.put("formatter.class", "com.datastrato.gravitino.audit.DummyAuditFormatter");
    return properties;
  }

  @Test
  public void formatCreateTableJson() throws JsonProcessingException {
    AuditLog auditLog = new AuditLogFormatter().format(createTableEvent);
    String json = JsonUtils.eventFieldMapper().writeValueAsString(auditLog);
    String expected =
        "{\"user\":\"user\",\"action\":\"create\","
            + "\"object_type\":\"table\","
            + "\"identifier\":\"b.c.d\","
            + "\"request\":{\"entity\":{\"name\":\"table\","
            + "\"columns\":[{\"name\":\"a\",\"data_type\":\"integer\","
            + "\"comment\":null,\"nullable\":true,\"auto_increment\":false,"
            + "\"default_value\":{}}],\"comment\":\"comment\","
            + "\"properties\":{\"a\":\"b\"},"
            + "\"partitions\":[{\"ref\":{\"field_name\":[\"a\"]}}],"
            + "\"distribution\":{\"strategy\":\"hash\","
            + "\"number\":10,\"expressions\":[{\"field_name\":[\"a\"]}]},"
            + "\"sort_orders\":[{\"expression\":{\"field_name\":[\"a\"]},"
            + "\"direction\":\"ascending\",\"null_ordering\":\"nulls_first\"}],"
            + "\"indexes\":[{\"indexType\":\"PRIMARY_KEY\",\"name\":\"p\","
            + "\"fieldNames\":[[\"a\"],[\"b\"]]}],\"audit_info\":null},\"changes\":null},"
            + "\"response\":{\"status\":\"success\",\"error_message\":null},"
            + "\"event_name\":\"CreateTableEvent\",\"timestamp\":%s}";
    Assertions.assertEquals(String.format(expected, createTableEvent.eventTime()), json);
  }

  @Test
  public void formatCreateTableFailedJson() throws JsonProcessingException {
    AuditLog auditLog = new AuditLogFormatter().format(createTableFailureEvent);
    String json = JsonUtils.eventFieldMapper().writeValueAsString(auditLog);
    String expected =
        "{\"user\":\"user\",\"action\":\"create\","
            + "\"object_type\":\"table\","
            + "\"identifier\":\"b.c.d\","
            + "\"request\":{\"entity\":{\"name\":\"table\","
            + "\"columns\":[{\"name\":\"a\",\"data_type\":\"integer\","
            + "\"comment\":null,\"nullable\":true,\"auto_increment\":false,"
            + "\"default_value\":{}}],\"comment\":\"comment\","
            + "\"properties\":{\"a\":\"b\"},"
            + "\"partitions\":[{\"ref\":{\"field_name\":[\"a\"]}}],"
            + "\"distribution\":{\"strategy\":\"hash\","
            + "\"number\":10,\"expressions\":[{\"field_name\":[\"a\"]}]},"
            + "\"sort_orders\":[{\"expression\":{\"field_name\":[\"a\"]},"
            + "\"direction\":\"ascending\",\"null_ordering\":\"nulls_first\"}],"
            + "\"indexes\":[{\"indexType\":\"PRIMARY_KEY\",\"name\":\"p\","
            + "\"fieldNames\":[[\"a\"],[\"b\"]]}],\"audit_info\":null},\"changes\":null},"
            + "\"response\":{\"status\":\"fail\",\"error_message\":\"error create table!\"},"
            + "\"event_name\":\"CreateTableFailureEvent\",\"timestamp\":%s}";
    Assertions.assertEquals(String.format(expected, createTableFailureEvent.eventTime()), json);
  }

  @Test
  public void formatJsonAlterTable() throws JsonProcessingException {
    AuditLog auditLog = new AuditLogFormatter().format(alterTableEvent);
    String json = JsonUtils.eventFieldMapper().writeValueAsString(auditLog);
    String expected =
        "{\"user\":\"user\",\"action\":\"alter\","
            + "\"object_type\":\"table\",\"identifier\":\"b.c.d\","
            + "\"request\":{\"entity\":{\"name\":\"table\","
            + "\"columns\":[{\"name\":\"a\",\"data_type\":\"long\","
            + "\"comment\":null,\"nullable\":true,\"auto_increment\":false,"
            + "\"default_value\":{}},{\"name\":\"b\",\"data_type\":\"integer\","
            + "\"comment\":null,\"nullable\":true,"
            + "\"auto_increment\":false,\"default_value\":{}}],"
            + "\"comment\":\"new comment\",\"properties\":{\"a\":\"b\",\"c\":\"d\"},"
            + "\"partitions\":[{\"ref\":{\"field_name\":[\"a\"]}}],"
            + "\"distribution\":{\"strategy\":\"hash\",\"number\":10,"
            + "\"expressions\":[{\"field_name\":[\"a\"]}]},"
            + "\"sort_orders\":[{\"expression\":{\"field_name\":[\"a\"]},"
            + "\"direction\":\"ascending\",\"null_ordering\":\"nulls_first\"}],"
            + "\"indexes\":[{\"indexType\":\"PRIMARY_KEY\",\"name\":\"p\","
            + "\"fieldNames\":[[\"a\"],[\"b\"]]}],\"audit_info\":null},"
            + "\"changes\":[{\"type\":\"AddColumn\","
            + "\"change\":{\"field_name\":[\"b\"],\"data_type\":\"integer\","
            + "\"comment\":null,\"position\":\"default\",\"nullable\":true,"
            + "\"auto_increment\":false,\"default_value\":{}}},"
            + "{\"type\":\"UpdateColumnType\",\"change\":{\"field_name\":[\"a\"],"
            + "\"new_data_type\":\"long\"}},{\"type\":\"SetProperty\","
            + "\"change\":{\"property\":\"c\",\"value\":\"d\"}},{\"type\":\"UpdateComment\","
            + "\"change\":{\"new_comment\":\"new comment\"}}]},"
            + "\"response\":{\"status\":\"success\",\"error_message\":null},"
            + "\"event_name\":\"AlterTableEvent\",\"timestamp\":%s}";
    Assertions.assertEquals(
        String.format(expected, alterTableEvent.eventTime()).length(), json.length());
  }

  @Test
  public void formatJsonAlterTableFail() throws JsonProcessingException {
    AuditLog auditLog = new AuditLogFormatter().format(alterTableFailureEvent);
    String json = JsonUtils.eventFieldMapper().writeValueAsString(auditLog);
    String expected =
        "{\"user\":\"user\",\"action\":\"alter\","
            + "\"object_type\":\"table\",\"identifier\":\"b.c.d\","
            + "\"request\":{\"entity\":null,"
            + "\"changes\":[{\"type\":\"AddColumn\","
            + "\"change\":{\"field_name\":[\"b\"],\"data_type\":\"integer\","
            + "\"comment\":null,\"position\":\"default\",\"nullable\":true,"
            + "\"auto_increment\":false,\"default_value\":{}}},"
            + "{\"type\":\"UpdateColumnType\",\"change\":{\"field_name\":[\"a\"],"
            + "\"new_data_type\":\"long\"}},{\"type\":\"SetProperty\","
            + "\"change\":{\"property\":\"c\",\"value\":\"d\"}},{\"type\":\"UpdateComment\","
            + "\"change\":{\"new_comment\":\"new comment\"}}]},"
            + "\"response\":{\"status\":\"fail\",\"error_message\":\"error alter table!\"},"
            + "\"event_name\":\"AlterTableFailureEvent\",\"timestamp\":%s}";
    Assertions.assertEquals(
        String.format(expected, alterTableFailureEvent.eventTime()).length(), json.length());
  }

  @Test
  public void formatJsonDropTable() throws JsonProcessingException {
    AuditLog auditLog = new AuditLogFormatter().format(dropTableEvent);
    String json = JsonUtils.eventFieldMapper().writeValueAsString(auditLog);
    String expected =
        "{\"user\":\"user\",\"action\":\"drop\","
            + "\"object_type\":\"table\",\"identifier\":\"b.c.d\","
            + "\"request\":null,\"response\":{\"status\":\"success\","
            + "\"error_message\":null},\"event_name\":\"DropTableEvent\",\"timestamp\":%d}";
    Assertions.assertEquals(String.format(expected, dropTableEvent.eventTime()), json);
  }

  @Test
  public void formatJsonDropTableFail() throws JsonProcessingException {
    AuditLog auditLog = new AuditLogFormatter().format(dropTableFailureEvent);
    String json = JsonUtils.eventFieldMapper().writeValueAsString(auditLog);
    String expected =
        "{\"user\":\"user\",\"action\":\"drop\","
            + "\"object_type\":\"table\",\"identifier\":\"b.c.d\","
            + "\"request\":null,\"response\":{\"status\":\"fail\","
            + "\"error_message\":\"error drop table!\"},\"event_name\":\"DropTableFailureEvent\",\"timestamp\":%d}";
    Assertions.assertEquals(String.format(expected, dropTableFailureEvent.eventTime()), json);
  }

  @Test
  public void tesFormatUnknownEvent() throws JsonProcessingException {
    AuditLog auditLog = new AuditLogFormatter().format(unknownEvent);
    String json = JsonUtils.eventFieldMapper().writeValueAsString(auditLog);
    String expected =
        "{\"user\":\"user\",\"action\":\"unknown\","
            + "\"object_type\":\"unknown\",\"identifier\":\"b.c.d\","
            + "\"request\":{\"entity\":null,\"changes\":null},"
            + "\"response\":{\"status\":\"success\",\"error_message\":null},"
            + "\"event_name\":\"DummyEvent\",\"timestamp\":%d}";
    Assertions.assertEquals(String.format(expected, unknownEvent.eventTime()), json);
  }

  @Test
  public void formatGetFilesetContextJson() throws JsonProcessingException {
    AuditLog auditLog = new AuditLogFormatter().format(getFilesetContextEvent);
    String json = JsonUtils.eventFieldMapper().writeValueAsString(auditLog);
    Long timestamp = getFilesetContextEvent.eventTime();
    String expected =
        String.format(
            "{\"user\":\"user\",\"action\":\"get_fileset_context\",\"object_type\":\"fileset\","
                + "\"identifier\":\"b.c.d\",\"request\":{\"entity\":{\"sub_path\":\"/test.txt\","
                + "\"operation\":\"create\",\"client_type\":\"hadoop_gvfs\",\"ip\":\"127.0.0.1\","
                + "\"source_engine_type\":\"spark\",\"app_id\":\"application_1_1\",\"extra_info\":null,"
                + "\"fileset_type\":\"managed\",\"storage_location\":\"file:/tmp/test/b/c/d\","
                + "\"actual_paths\":[\"file:/tmp/test/b/c/d/test.txt\"]},"
                + "\"changes\":null},\"response\":{\"status\":\"success\",\"error_message\":null},"
                + "\"event_name\":\"GetFilesetContextEvent\",\"timestamp\":%d}",
            timestamp);
    Assertions.assertEquals(expected, json);
  }

  @Test
  public void formatGetFilesetFailureContextJson() throws JsonProcessingException {
    AuditLog auditLog = new AuditLogFormatter().format(getFilesetContextFailureEvent);
    String json = JsonUtils.eventFieldMapper().writeValueAsString(auditLog);
    Long timestamp = getFilesetContextFailureEvent.eventTime();
    String expected =
        String.format(
            "{\"user\":\"user\",\"action\":\"get_fileset_context\",\"object_type\":\"fileset\","
                + "\"identifier\":\"b.c.d\",\"request\":{\"entity\":{\"sub_path\":\"/test.txt\","
                + "\"operation\":\"create\",\"client_type\":\"hadoop_gvfs\",\"ip\":\"127.0.0.1\","
                + "\"source_engine_type\":\"spark\",\"app_id\":\"application_1_1\",\"extra_info\":null},"
                + "\"changes\":null},\"response\":{\"status\":\"fail\",\"error_message\":\"test\"},"
                + "\"event_name\":\"GetFilesetContextFailureEvent\",\"timestamp\":%d}",
            timestamp);
    Assertions.assertEquals(expected, json);
  }

  private CreateTableEvent mockCreateTableEvent() {
    CreateTableEvent event =
        new CreateTableEvent(
            "user", NameIdentifier.of("a", "b", "c", "d"), new TableInfo(mockTable()));
    return event;
  }

  private AlterTableEvent mockAlterTableEvent() {
    TableChange[] tableChanges =
        new TableChange[] {
          TableChange.addColumn(new String[] {"b"}, Types.IntegerType.get()),
          TableChange.updateColumnType(new String[] {"a"}, Types.LongType.get()),
          TableChange.setProperty("c", "d"),
          TableChange.updateComment("new comment")
        };
    return new AlterTableEvent(
        "user",
        NameIdentifier.of("a", "b", "c", "d"),
        tableChanges,
        new TableInfo(mockAlteredTable()));
  }

  private com.datastrato.gravitino.rel.Table mockTable() {
    com.datastrato.gravitino.rel.Table table = mock(com.datastrato.gravitino.rel.Table.class);
    when(table.name()).thenReturn("table");
    when(table.comment()).thenReturn("comment");
    when(table.properties()).thenReturn(ImmutableMap.of("a", "b"));
    when(table.columns()).thenReturn(new Column[] {Column.of("a", Types.IntegerType.get())});
    when(table.distribution())
        .thenReturn(Distributions.of(Strategy.HASH, 10, NamedReference.field("a")));
    when(table.index())
        .thenReturn(new Index[] {Indexes.primary("p", new String[][] {{"a"}, {"b"}})});
    when(table.sortOrder())
        .thenReturn(new SortOrder[] {SortOrders.ascending(NamedReference.field("a"))});
    when(table.partitioning()).thenReturn(new Transform[] {Transforms.identity("a")});
    when(table.auditInfo()).thenReturn(null);
    return table;
  }

  private com.datastrato.gravitino.rel.Table mockAlteredTable() {
    com.datastrato.gravitino.rel.Table table = mock(com.datastrato.gravitino.rel.Table.class);
    when(table.name()).thenReturn("table");
    when(table.comment()).thenReturn("new comment");
    when(table.properties()).thenReturn(ImmutableMap.of("a", "b", "c", "d"));
    when(table.columns())
        .thenReturn(
            new Column[] {
              Column.of("a", Types.LongType.get()), Column.of("b", Types.IntegerType.get())
            });
    when(table.distribution())
        .thenReturn(Distributions.of(Strategy.HASH, 10, NamedReference.field("a")));
    when(table.index())
        .thenReturn(new Index[] {Indexes.primary("p", new String[][] {{"a"}, {"b"}})});
    when(table.sortOrder())
        .thenReturn(new SortOrder[] {SortOrders.ascending(NamedReference.field("a"))});
    when(table.partitioning()).thenReturn(new Transform[] {Transforms.identity("a")});
    when(table.auditInfo()).thenReturn(null);
    return table;
  }

  private DropTableEvent mockDropTableEvent() {
    DropTableEvent event = new DropTableEvent("user", NameIdentifier.of("a", "b", "c", "d"), true);
    return event;
  }

  private CreateTableFailureEvent mockCreateTableFailureEvent() {
    CreateTableFailureEvent event =
        new CreateTableFailureEvent(
            "user",
            NameIdentifier.of("a", "b", "c", "d"),
            new Exception("error create table!"),
            new TableInfo(mockTable()));
    return event;
  }

  private AlterTableFailureEvent mockAlterTableFailEvent() {
    TableChange[] tableChanges =
        new TableChange[] {
          TableChange.addColumn(new String[] {"b"}, Types.IntegerType.get()),
          TableChange.updateColumnType(new String[] {"a"}, Types.LongType.get()),
          TableChange.setProperty("c", "d"),
          TableChange.updateComment("new comment")
        };
    return new AlterTableFailureEvent(
        "user",
        NameIdentifier.of("a", "b", "c", "d"),
        new Exception("error alter table!"),
        tableChanges);
  }

  private DropTableFailureEvent mockDropTableFailEvent() {
    DropTableFailureEvent event =
        new DropTableFailureEvent(
            "user", NameIdentifier.of("a", "b", "c", "d"), new Exception("error drop table!"));
    return event;
  }

  private DummyEvent mockUnknownEvent() {
    return new DummyEvent("user", NameIdentifier.of("a", "b", "c", "d"));
  }

  private GetFilesetContextEvent mockGetFilesetContextEvent() {
    FilesetDataOperationCtx ctx =
        BaseFilesetDataOperationCtx.builder()
            .withSubPath("/test.txt")
            .withOperation(FilesetDataOperation.CREATE)
            .withClientType(ClientType.HADOOP_GVFS)
            .withIp("127.0.0.1")
            .withSourceEngineType(SourceEngineType.SPARK)
            .withAppId("application_1_1")
            .build();
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    Fileset fileset =
        TestFileset.builder()
            .withName("d")
            .withType(Fileset.Type.MANAGED)
            .withComment("this is test")
            .withStorageLocation("file:/tmp/test/b/c/d")
            .withProperties(Maps.newHashMap())
            .withAuditInfo(auditInfo)
            .build();
    String[] actualPaths = new String[] {"file:/tmp/test/b/c/d/test.txt"};
    FilesetContext filesetContext =
        TestFilesetContext.builder().withFileset(fileset).withActualPaths(actualPaths).build();
    return new GetFilesetContextEvent(
        "user", NameIdentifier.ofFileset("a", "b", "c", "d"), ctx, filesetContext);
  }

  private GetFilesetContextFailureEvent mockGetFilesetContextFailureEvent() {
    FilesetDataOperationCtx ctx =
        BaseFilesetDataOperationCtx.builder()
            .withSubPath("/test.txt")
            .withOperation(FilesetDataOperation.CREATE)
            .withClientType(ClientType.HADOOP_GVFS)
            .withIp("127.0.0.1")
            .withSourceEngineType(SourceEngineType.SPARK)
            .withAppId("application_1_1")
            .build();
    return new GetFilesetContextFailureEvent(
        "user",
        NameIdentifier.ofFileset("a", "b", "c", "d"),
        ctx,
        new GravitinoRuntimeException("test"));
  }

  private EventListenerManager mockEventListenerManager() {
    EventListenerManager eventListenerManager = new EventListenerManager();
    eventListenerManager.init(new HashMap<>());
    eventListenerManager.start();
    return eventListenerManager;
  }

  private AuditLogManager mockAndDispatchEvent(Event event) {
    EventListenerManager eventListenerManager = mockEventListenerManager();
    EventBus eventBus = eventListenerManager.createEventBus();
    AuditLogManager auditLogManager = mockAuditLogManager(eventListenerManager);
    eventBus.dispatchEvent(event);
    return auditLogManager;
  }

  private AuditLogManager mockAuditLogManager(EventListenerManager eventListenerManager) {
    AuditLogManager auditLogManager = new AuditLogManager();
    Map<String, String> config = createManagerConfig();
    auditLogManager.init(config, eventListenerManager);
    return auditLogManager;
  }

  static class DummyEvent extends Event {
    protected DummyEvent(String user, NameIdentifier identifier) {
      super(user, identifier);
    }
  }
}
