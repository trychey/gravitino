/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.audit;

import com.datastrato.gravitino.listener.api.event.Event;

public class AuditLog<E, C> {
  private String user;

  private Action action;

  private ObjectType objectType;

  private String identifier;

  private Request<E, C> request;

  private Response response;

  private String eventName;

  private Long timestamp;

  public AuditLog() {};

  public AuditLog(
      String user,
      Action action,
      ObjectType objectType,
      String identifier,
      Request request,
      Response response,
      String eventName,
      Long timestamp) {
    this.user = user;
    this.action = action;
    this.objectType = objectType;
    this.identifier = identifier;
    this.request = request;
    this.response = response;
    this.eventName = eventName;
    this.timestamp = timestamp;
  }

  public String getUser() {
    return user;
  }

  public Action getAction() {
    return action;
  }

  public ObjectType getObjectType() {
    return objectType;
  }

  public String getIdentifier() {
    return identifier;
  }

  public Request<E, C> getRequest() {
    return request;
  }

  public Response getResponse() {
    return response;
  }

  public String getEventName() {
    return eventName;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public static class Request<E, C> {
    private E entity;
    private ChangeInfo<C>[] changes;

    public Request(E entity, ChangeInfo<C>[] changes) {
      this.entity = entity;
      this.changes = changes;
    }

    public Request() {}

    public Request(E entity) {
      this.entity = entity;
    }

    public E getEntity() {
      return entity;
    }

    public ChangeInfo<C>[] getChanges() {
      return changes;
    }
  }

  public static class Response {
    private Status status;

    private String errorMessage;

    public static Response ofSuccess() {
      return new Response(Status.SUCCESS, null);
    }

    public static Response ofFailure(String errorMessage) {
      return new Response(Status.FAIL, errorMessage);
    }

    private Response(Status status, String errorMessage) {
      this.status = status;
      this.errorMessage = errorMessage;
    }

    public Status getStatus() {
      return status;
    }

    public String getErrorMessage() {
      return errorMessage;
    }
  }

  public static class ChangeInfo<C> {
    private String type;

    private C change;

    public ChangeInfo(String type, C change) {
      this.type = type;
      this.change = change;
    }

    public String getType() {
      return type;
    }

    public C getChange() {
      return change;
    }
  }

  public enum ObjectType {
    METALAKE,
    CATALOG,
    SCHEMA,
    TABLE,
    TOPIC,
    FILESET,
    UNKNOWN
  }

  public enum Action {
    CREATE,
    ALTER,
    DROP,
    PURGE,
    LIST,
    LOAD,
    GET_FILESET_CONTEXT,
    UNKNOWN;

    public static Action from(Event event) {
      String eventClassName = event.getClass().getSimpleName();
      if (eventClassName.startsWith("Create")) {
        return CREATE;
      } else if (eventClassName.startsWith("Alter")) {
        return ALTER;
      } else if (eventClassName.startsWith("Drop")) {
        return DROP;
      } else if (eventClassName.startsWith("Purge")) {
        return PURGE;
      } else if (eventClassName.startsWith("List")) {
        return LIST;
      } else if (eventClassName.startsWith("Load")) {
        return LOAD;
      } else if (eventClassName.startsWith("GetFilesetContext")) {
        return GET_FILESET_CONTEXT;
      } else {
        return UNKNOWN;
      }
    }
  }

  public enum TAG {
    METADATA_WRITE,
    METADATA_READ;

    public static TAG from(Action action) {
      switch (action) {
        case CREATE:
        case ALTER:
        case DROP:
        case PURGE:
          return METADATA_WRITE;
        default:
          return METADATA_READ;
      }
    }
  }

  public enum Status {
    SUCCESS,
    FAIL
  }
}
