/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.meta;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.EntityStore;
import com.datastrato.graviton.MetalakeChange;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.TestEntityStore;
import com.datastrato.graviton.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestMetalakeManager {

  private static MetalakeManager metalakeManager;

  private static EntityStore entityStore;

  private static Config config;

  @BeforeAll
  public static void setUp() {
    config = new Config(false) {};

    entityStore = new TestEntityStore.InMemoryEntityStore();
    entityStore.initialize(config);
    entityStore.setSerDeFacade(null);

    metalakeManager = new MetalakeManager(entityStore);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (entityStore != null) {
      entityStore.close();
      entityStore = null;
    }
  }

  @Test
  public void testCreateMetalake() {
    NameIdentifier ident = NameIdentifier.of("test1");
    Map<String, String> props = ImmutableMap.of("key1", "value1");

    BaseMetalake metalake = metalakeManager.createMetalake(ident, "comment", props);
    Assertions.assertEquals("test1", metalake.name());
    Assertions.assertEquals("comment", metalake.comment());
    Assertions.assertEquals(props, metalake.properties());

    // Test with MetalakeAlreadyExistsException
    Assertions.assertThrows(
        MetalakeAlreadyExistsException.class,
        () -> metalakeManager.createMetalake(ident, "comment", props));
  }

  @Test
  public void testListMetalakes() {
    NameIdentifier ident1 = NameIdentifier.of("test11");
    NameIdentifier ident2 = NameIdentifier.of("test12");
    Map<String, String> props = ImmutableMap.of("key1", "value1");

    BaseMetalake metalake1 = metalakeManager.createMetalake(ident1, "comment", props);
    BaseMetalake metalake2 = metalakeManager.createMetalake(ident2, "comment", props);

    Set<BaseMetalake> metalakes = Sets.newHashSet(metalakeManager.listMetalakes());
    Assertions.assertTrue(metalakes.contains(metalake1));
    Assertions.assertTrue(metalakes.contains(metalake2));
  }

  @Test
  public void testLoadMetalake() {
    NameIdentifier ident = NameIdentifier.of("test21");
    Map<String, String> props = ImmutableMap.of("key1", "value1");

    BaseMetalake metalake = metalakeManager.createMetalake(ident, "comment", props);
    Assertions.assertEquals("test21", metalake.name());
    Assertions.assertEquals("comment", metalake.comment());
    Assertions.assertEquals(props, metalake.properties());

    BaseMetalake loadedMetalake = metalakeManager.loadMetalake(ident);
    Assertions.assertEquals("test21", loadedMetalake.name());
    Assertions.assertEquals("comment", loadedMetalake.comment());
    Assertions.assertEquals(props, loadedMetalake.properties());

    // Test with NoSuchMetalakeException
    Throwable exception =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () -> metalakeManager.loadMetalake(NameIdentifier.of("test3")));
    Assertions.assertTrue(exception.getMessage().contains("Metalake test3 does not exist"));
  }

  @Test
  public void testAlterMetalake() {
    NameIdentifier ident = NameIdentifier.of("test31");
    Map<String, String> props = ImmutableMap.of("key1", "value1");

    BaseMetalake metalake = metalakeManager.createMetalake(ident, "comment", props);
    Assertions.assertEquals("test31", metalake.name());
    Assertions.assertEquals("comment", metalake.comment());
    Assertions.assertEquals(props, metalake.properties());

    // Test alter name;
    MetalakeChange change = MetalakeChange.rename("test32");
    BaseMetalake alteredMetalake = metalakeManager.alterMetalake(ident, change);
    Assertions.assertEquals("test32", alteredMetalake.name());
    Assertions.assertEquals("comment", alteredMetalake.comment());
    Assertions.assertEquals(props, alteredMetalake.properties());

    // Test alter comment;
    NameIdentifier ident1 = NameIdentifier.of("test32");
    MetalakeChange change1 = MetalakeChange.updateComment("comment2");
    BaseMetalake alteredMetalake1 = metalakeManager.alterMetalake(ident1, change1);
    Assertions.assertEquals("test32", alteredMetalake1.name());
    Assertions.assertEquals("comment2", alteredMetalake1.comment());
    Assertions.assertEquals(props, alteredMetalake1.properties());

    // test alter properties;
    MetalakeChange change2 = MetalakeChange.setProperty("key2", "value2");
    MetalakeChange change3 = MetalakeChange.setProperty("key3", "value3");
    MetalakeChange change4 = MetalakeChange.removeProperty("key3");

    BaseMetalake alteredMetalake2 =
        metalakeManager.alterMetalake(ident1, change2, change3, change4);
    Assertions.assertEquals("test32", alteredMetalake2.name());
    Assertions.assertEquals("comment2", alteredMetalake2.comment());
    Map<String, String> expectedProps = ImmutableMap.of("key1", "value1", "key2", "value2");
    Assertions.assertEquals(expectedProps, alteredMetalake2.properties());

    // Test with NoSuchMetalakeException
    Throwable exception =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () -> metalakeManager.alterMetalake(NameIdentifier.of("test3"), change));
    Assertions.assertTrue(exception.getMessage().contains("Metalake test3 does not exist"));
  }

  @Test
  public void testDropMetalake() {
    NameIdentifier ident = NameIdentifier.of("test41");
    Map<String, String> props = ImmutableMap.of("key1", "value1");

    BaseMetalake metalake = metalakeManager.createMetalake(ident, "comment", props);
    Assertions.assertEquals("test41", metalake.name());
    Assertions.assertEquals("comment", metalake.comment());
    Assertions.assertEquals(props, metalake.properties());

    boolean dropped = metalakeManager.dropMetalake(ident);
    Assertions.assertTrue(dropped);

    // Test with NoSuchMetalakeException
    NameIdentifier ident1 = NameIdentifier.of("test42");
    boolean dropped1 = metalakeManager.dropMetalake(ident1);
    Assertions.assertFalse(dropped1);
  }
}
