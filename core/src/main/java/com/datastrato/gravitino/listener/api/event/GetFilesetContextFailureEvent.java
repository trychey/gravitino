/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.file.FilesetDataOperationCtx;

/**
 * Represents an event that is generated when an attempt to get a fileset context from the system
 * fails.
 */
@DeveloperApi
public final class GetFilesetContextFailureEvent extends FilesetFailureEvent {
  private final FilesetDataOperationCtx ctx;
  /**
   * Constructs a new {@code GetFilesetContextFailureEvent}.
   *
   * @param user The user who initiated the get fileset context operation.
   * @param identifier The identifier of the fileset context that was attempted to be got.
   * @param ctx The data operation context to get the fileset context.
   * @param exception The exception that was thrown during the get fileset context operation. This
   *     exception is key to diagnosing the failure, providing insights into what went wrong during
   *     the operation.
   */
  public GetFilesetContextFailureEvent(
      String user, NameIdentifier identifier, FilesetDataOperationCtx ctx, Exception exception) {
    super(user, identifier, exception);
    this.ctx = ctx;
  }

  public FilesetDataOperationCtx dataOperationContext() {
    return ctx;
  }
}
