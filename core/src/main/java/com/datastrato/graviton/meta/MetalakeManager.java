/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.meta;

import com.datastrato.graviton.Entity.EntityType;
import com.datastrato.graviton.EntityAlreadyExistsException;
import com.datastrato.graviton.EntityStore;
import com.datastrato.graviton.MetalakeChange;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.SupportsMetalakes;
import com.datastrato.graviton.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchEntityException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages Metalakes within the Graviton system. */
public class MetalakeManager implements SupportsMetalakes {

  private static final Logger LOG = LoggerFactory.getLogger(MetalakeManager.class);

  private final EntityStore store;

  /**
   * Constructs a MetalakeManager instance.
   *
   * @param store The EntityStore to use for managing Metalakes.
   */
  public MetalakeManager(EntityStore store) {
    this.store = store;
  }

  /**
   * Lists all available Metalakes.
   *
   * @return An array of Metalake instances representing the available Metalakes.
   * @throws RuntimeException If listing Metalakes encounters storage issues.
   */
  @Override
  public BaseMetalake[] listMetalakes() {
    try {
      return store
          .list(Namespace.empty(), BaseMetalake.class, EntityType.METALAKE)
          .toArray(new BaseMetalake[0]);
    } catch (IOException ioe) {
      LOG.error("Listing Metalakes failed due to storage issues.", ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Loads a Metalake.
   *
   * @param ident The identifier of the Metalake to load.
   * @return The loaded Metalake instance.
   * @throws NoSuchMetalakeException If the Metalake with the given identifier does not exist.
   * @throws RuntimeException If loading the Metalake encounters storage issues.
   */
  @Override
  public BaseMetalake loadMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    try {
      return store.get(ident, EntityType.METALAKE, BaseMetalake.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("Metalake {} does not exist", ident, e);
      throw new NoSuchMetalakeException("Metalake " + ident + " does not exist");
    } catch (IOException ioe) {
      LOG.error("Loading Metalake {} failed due to storage issues", ident, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Creates a new Metalake.
   *
   * @param ident The identifier of the new Metalake.
   * @param comment A comment or description for the Metalake.
   * @param properties Additional properties for the Metalake.
   * @return The created Metalake instance.
   * @throws MetalakeAlreadyExistsException If a Metalake with the same identifier already exists.
   * @throws RuntimeException If creating the Metalake encounters storage issues.
   */
  @Override
  public BaseMetalake createMetalake(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws MetalakeAlreadyExistsException {
    BaseMetalake metalake =
        new BaseMetalake.Builder()
            .withId(1L /* TODO: Use ID generator */)
            .withName(ident.name())
            .withComment(comment)
            .withProperties(properties)
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(
                new AuditInfo.Builder()
                    .withCreator("graviton") /*TODO: Use real user later on.  */
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    try {
      store.put(metalake, false /* overwritten */);
      return metalake;
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("Metalake {} already exists", ident, e);
      throw new MetalakeAlreadyExistsException("Metalake " + ident + " already exists");
    } catch (IOException ioe) {
      LOG.error("Loading Metalake {} failed due to storage issues", ident, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Alters a Metalake by applying specified changes.
   *
   * @param ident The identifier of the Metalake to be altered.
   * @param changes The array of MetalakeChange objects representing the changes to apply.
   * @return The altered Metalake instance after applying the changes.
   * @throws NoSuchMetalakeException If the Metalake with the given identifier does not exist.
   * @throws IllegalArgumentException If the provided changes are invalid.
   * @throws RuntimeException If altering the Metalake encounters storage issues.
   */
  @Override
  public BaseMetalake alterMetalake(NameIdentifier ident, MetalakeChange... changes)
      throws NoSuchMetalakeException, IllegalArgumentException {
    try {
      return store.update(
          ident,
          BaseMetalake.class,
          EntityType.METALAKE,
          metalake -> {
            BaseMetalake.Builder builder =
                new BaseMetalake.Builder()
                    .withId(metalake.getId())
                    .withName(metalake.name())
                    .withComment(metalake.comment())
                    .withProperties(metalake.properties())
                    .withVersion(metalake.getVersion());

            AuditInfo newInfo =
                new AuditInfo.Builder()
                    .withCreator(metalake.auditInfo().creator())
                    .withCreateTime(metalake.auditInfo().createTime())
                    .withLastModifier(
                        metalake.auditInfo().creator()) /*TODO: Use real user later on.  */
                    .withLastModifiedTime(Instant.now())
                    .build();
            builder.withAuditInfo(newInfo);

            Map<String, String> newProps =
                metalake.properties() == null
                    ? Maps.newHashMap()
                    : Maps.newHashMap(metalake.properties());
            builder = updateEntity(builder, newProps, changes);

            return builder.build();
          });

    } catch (NoSuchEntityException ne) {
      LOG.warn("Metalake {} does not exist", ident, ne);
      throw new NoSuchMetalakeException("Metalake " + ident + " does not exist");

    } catch (IllegalArgumentException iae) {
      LOG.warn("Altering Metalake {} failed due to invalid changes", ident, iae);
      throw iae;

    } catch (IOException ioe) {
      LOG.error("Loading Metalake {} failed due to storage issues", ident, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Deletes a Metalake.
   *
   * @param ident The identifier of the Metalake to be deleted.
   * @return `true` if the Metalake was successfully deleted, `false` otherwise.
   * @throws RuntimeException If deleting the Metalake encounters storage issues.
   */
  @Override
  public boolean dropMetalake(NameIdentifier ident) {
    try {
      return store.delete(ident, EntityType.METALAKE);
    } catch (IOException ioe) {
      LOG.error("Deletinf metalake {} failed due to storage issues", ident, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Updates an entity with the provided changes.
   *
   * @param builder The builder for the entity.
   * @param newProps The new properties to apply.
   * @param changes The changes to apply.
   * @return The updated entity builder.
   * @throws IllegalArgumentException If an unknown MetalakeChange is encountered.
   */
  private BaseMetalake.Builder updateEntity(
      BaseMetalake.Builder builder, Map<String, String> newProps, MetalakeChange... changes) {
    for (MetalakeChange change : changes) {
      if (change instanceof MetalakeChange.RenameMetalake) {
        MetalakeChange.RenameMetalake rename = (MetalakeChange.RenameMetalake) change;
        builder.withName(rename.getNewName());

      } else if (change instanceof MetalakeChange.UpdateMetalakeComment) {
        MetalakeChange.UpdateMetalakeComment comment =
            (MetalakeChange.UpdateMetalakeComment) change;
        builder.withComment(comment.getNewComment());

      } else if (change instanceof MetalakeChange.SetProperty) {
        MetalakeChange.SetProperty setProperty = (MetalakeChange.SetProperty) change;
        newProps.put(setProperty.getProperty(), setProperty.getValue());

      } else if (change instanceof MetalakeChange.RemoveProperty) {
        MetalakeChange.RemoveProperty removeProperty = (MetalakeChange.RemoveProperty) change;
        newProps.remove(removeProperty.getProperty());

      } else {
        throw new IllegalArgumentException("Unknown metalake change type: " + change);
      }
    }

    return builder.withProperties(newProps);
  }
}
