/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.file.FilesetContext;
import com.datastrato.gravitino.file.FilesetDataOperationCtx;

/** Represents an event that occurs when getting a fileset context. */
@DeveloperApi
public final class GetFilesetContextEvent extends FilesetEvent {
  private final FilesetDataOperationCtx ctx;
  private final FilesetContext filesetContext;

  /**
   * Constructs a new {@code GetFilesetContextEvent}, recording the attempt to get a fileset
   * context.
   *
   * @param user The user who initiated the get fileset context operation.
   * @param identifier The identifier of the fileset context that was attempted to be got.
   * @param ctx The data operation context to get the fileset context.
   * @param filesetContext The fileset context.
   */
  public GetFilesetContextEvent(
      String user,
      NameIdentifier identifier,
      FilesetDataOperationCtx ctx,
      FilesetContext filesetContext) {
    super(user, identifier);
    this.ctx = ctx;
    this.filesetContext = filesetContext;
  }

  public FilesetDataOperationCtx dataOperationContext() {
    return ctx;
  }

  public FilesetContext filesetContext() {
    return filesetContext;
  }
}
