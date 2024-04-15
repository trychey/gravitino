/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.CatalogDispatcher;
import com.datastrato.gravitino.catalog.CatalogEventDispatcher;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.listener.DummyEventListener;
import com.datastrato.gravitino.listener.EventBus;
import com.datastrato.gravitino.listener.api.info.CatalogInfo;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestCatalogEvent {
  private CatalogEventDispatcher dispatcher;
  private CatalogEventDispatcher failureDispatcher;
  private DummyEventListener dummyEventListener;
  private Catalog catalog;

  @BeforeAll
  void init() {
    this.catalog = mockCatalog();
    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Arrays.asList(dummyEventListener));
    CatalogDispatcher catalogDispatcher = mockCatalogDispatcher();
    this.dispatcher = new CatalogEventDispatcher(eventBus, catalogDispatcher);
    CatalogDispatcher catalogExceptionDispatcher = mockExceptionCatalogDispatcher();
    this.failureDispatcher = new CatalogEventDispatcher(eventBus, catalogExceptionDispatcher);
  }

  @Test
  void testCreateCatalog() {
    NameIdentifier identifier = NameIdentifier.of("metalake", catalog.name());
    dispatcher.createCatalog(
        identifier, catalog.type(), catalog.provider(), catalog.comment(), catalog.properties());
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateCatalogEvent.class, event.getClass());
    CatalogInfo catalogInfo = ((CreateCatalogEvent) event).createdCatalogInfo();
    checkCatalogInfo(catalogInfo, catalog);
  }

  @Test
  void testLoadCatalog() {
    NameIdentifier identifier = NameIdentifier.of("metalake", catalog.name());
    dispatcher.loadCatalog(identifier);
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadCatalogEvent.class, event.getClass());
    CatalogInfo catalogInfo = ((LoadCatalogEvent) event).loadedCatalogInfo();
    checkCatalogInfo(catalogInfo, catalog);
  }

  @Test
  void testAlterCatalog() {
    NameIdentifier identifier = NameIdentifier.of("metalake", catalog.name());
    CatalogChange catalogChange = CatalogChange.setProperty("a", "b");
    dispatcher.alterCatalog(identifier, catalogChange);
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterCatalogEvent.class, event.getClass());
    CatalogInfo catalogInfo = ((AlterCatalogEvent) event).updatedCatalogInfo();
    checkCatalogInfo(catalogInfo, catalog);
    CatalogChange[] catalogChanges = ((AlterCatalogEvent) event).catalogChanges();
    Assertions.assertEquals(1, catalogChanges.length);
    Assertions.assertEquals(catalogChange, catalogChanges[0]);
  }

  @Test
  void testDropCatalog() {
    NameIdentifier identifier = NameIdentifier.of("metalake", catalog.name());
    dispatcher.dropCatalog(identifier);
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropCatalogEvent.class, event.getClass());
    Assertions.assertEquals(true, ((DropCatalogEvent) event).isExists());
  }

  @Test
  void testListCatalog() {
    Namespace namespace = Namespace.of("metalake");
    dispatcher.listCatalogs(namespace);
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(namespace.toString(), event.identifier().toString());
    Assertions.assertEquals(ListCatalogEvent.class, event.getClass());
    Assertions.assertEquals(namespace, ((ListCatalogEvent) event).namespace());
  }

  @Test
  void testListCatalogInfo() {
    Namespace namespace = Namespace.of("metalake");
    dispatcher.listCatalogsInfo(namespace);
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(namespace.toString(), event.identifier().toString());
    Assertions.assertEquals(ListCatalogEvent.class, event.getClass());
    Assertions.assertEquals(namespace, ((ListCatalogEvent) event).namespace());
  }

  @Test
  void testCreateCatalogFailure() {
    NameIdentifier identifier = NameIdentifier.of("metalake", catalog.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.createCatalog(
                identifier,
                catalog.type(),
                catalog.provider(),
                catalog.comment(),
                catalog.properties()));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateCatalogFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((CreateCatalogFailureEvent) event).exception().getClass());
    checkCatalogInfo(((CreateCatalogFailureEvent) event).createCatalogRequest(), catalog);
  }

  @Test
  void testLoadCatalogFailure() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.loadCatalog(identifier));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadCatalogFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((LoadCatalogFailureEvent) event).exception().getClass());
  }

  @Test
  void testAlterCatalogFailure() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog");
    CatalogChange catalogChange = CatalogChange.setProperty("a", "b");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.alterCatalog(identifier, catalogChange));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterCatalogFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((AlterCatalogFailureEvent) event).exception().getClass());
    CatalogChange[] catalogChanges = ((AlterCatalogFailureEvent) event).catalogChanges();
    Assertions.assertEquals(1, catalogChanges.length);
    Assertions.assertEquals(catalogChange, catalogChanges[0]);
  }

  @Test
  void testDropCatalogFailure() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.dropCatalog(identifier));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropCatalogFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((DropCatalogFailureEvent) event).exception().getClass());
  }

  @Test
  void testListCatalogFailure() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listCatalogs(namespace));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(ListCatalogFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListCatalogFailureEvent) event).exception().getClass());
    Assertions.assertEquals(namespace, ((ListCatalogFailureEvent) event).namespace());
  }

  @Test
  void testListCatalogInfoFailure() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listCatalogsInfo(namespace));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(ListCatalogFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListCatalogFailureEvent) event).exception().getClass());
    Assertions.assertEquals(namespace, ((ListCatalogFailureEvent) event).namespace());
  }

  private void checkCatalogInfo(CatalogInfo catalogInfo, Catalog catalog) {
    Assertions.assertEquals(catalog.name(), catalogInfo.name());
    Assertions.assertEquals(catalog.type(), catalogInfo.type());
    Assertions.assertEquals(catalog.provider(), catalogInfo.provider());
    Assertions.assertEquals(catalog.properties(), catalogInfo.properties());
    Assertions.assertEquals(catalog.comment(), catalogInfo.comment());
  }

  private Catalog mockCatalog() {
    Catalog catalog = mock(Catalog.class);
    when(catalog.comment()).thenReturn("comment");
    when(catalog.properties()).thenReturn(ImmutableMap.of("a", "b"));
    when(catalog.name()).thenReturn("catalog");
    when(catalog.provider()).thenReturn("hive");
    when(catalog.type()).thenReturn(Catalog.Type.RELATIONAL);
    when(catalog.auditInfo()).thenReturn(null);
    return catalog;
  }

  private CatalogDispatcher mockCatalogDispatcher() {
    CatalogDispatcher dispatcher = mock(CatalogDispatcher.class);
    when(dispatcher.createCatalog(
            any(NameIdentifier.class),
            any(Catalog.Type.class),
            any(String.class),
            any(String.class),
            any(Map.class)))
        .thenReturn(catalog);
    when(dispatcher.loadCatalog(any(NameIdentifier.class))).thenReturn(catalog);
    when(dispatcher.dropCatalog(any(NameIdentifier.class))).thenReturn(true);
    when(dispatcher.listCatalogs(any(Namespace.class))).thenReturn(null);
    when(dispatcher.alterCatalog(any(NameIdentifier.class), any(CatalogChange.class)))
        .thenReturn(catalog);
    return dispatcher;
  }

  private CatalogDispatcher mockExceptionCatalogDispatcher() {
    CatalogDispatcher dispatcher =
        mock(
            CatalogDispatcher.class,
            invocation -> {
              throw new GravitinoRuntimeException("Exception for all methods");
            });
    return dispatcher;
  }
}
