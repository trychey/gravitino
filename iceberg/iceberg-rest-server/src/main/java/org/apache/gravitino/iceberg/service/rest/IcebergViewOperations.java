package org.apache.gravitino.iceberg.service.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.iceberg.service.IcebergRestUtils;
import org.apache.gravitino.iceberg.service.IcebergTableOpsManager;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;

@Path("/v1/{prefix:([^/]*/)?}namespaces/{namespace}/views")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class IcebergViewOperations {

  private IcebergTableOpsManager icebergTableOpsManager;

  @SuppressWarnings("UnusedVariable")
  @Context
  private HttpServletRequest httpRequest;

  @Inject
  public IcebergViewOperations(IcebergTableOpsManager icebergTableOpsManager) {
    this.icebergTableOpsManager = icebergTableOpsManager;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "list-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-view", absolute = true)
  public Response listView(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace) {
    ListTablesResponse response =
        icebergTableOpsManager.getOps(prefix).listView(RESTUtil.decodeNamespace(namespace));
    return IcebergRestUtils.ok(response);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "create-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-view", absolute = true)
  public Response createView(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      CreateViewRequest request) {
    LoadViewResponse response =
        icebergTableOpsManager
            .getOps(prefix)
            .createView(RESTUtil.decodeNamespace(namespace), request);
    return IcebergRestUtils.ok(response);
  }

  @GET
  @Path("{view}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "load-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-view", absolute = true)
  public Response loadView(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    TableIdentifier viewIdentifier = TableIdentifier.of(RESTUtil.decodeNamespace(namespace), view);
    LoadViewResponse response = icebergTableOpsManager.getOps(prefix).loadView(viewIdentifier);
    return IcebergRestUtils.ok(response);
  }

  @POST
  @Path("{view}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "replace-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "replace-view", absolute = true)
  public Response replaceView(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view,
      UpdateTableRequest request) {
    TableIdentifier viewIdentifier = TableIdentifier.of(RESTUtil.decodeNamespace(namespace), view);
    LoadViewResponse response =
        icebergTableOpsManager.getOps(prefix).updateView(viewIdentifier, request);
    return IcebergRestUtils.ok(response);
  }

  @DELETE
  @Path("{view}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "drop-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-view", absolute = true)
  public Response dropView(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    TableIdentifier viewIdentifier = TableIdentifier.of(RESTUtil.decodeNamespace(namespace), view);
    icebergTableOpsManager.getOps(prefix).dropView(viewIdentifier);
    return IcebergRestUtils.noContent();
  }

  @HEAD
  @Path("{view}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "view-exists." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "view-exits", absolute = true)
  public Response tableExists(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    TableIdentifier tableIdentifier = TableIdentifier.of(RESTUtil.decodeNamespace(namespace), view);
    if (icebergTableOpsManager.getOps(prefix).existView(tableIdentifier)) {
      return IcebergRestUtils.noContent();
    } else {
      return IcebergRestUtils.notExists();
    }
  }

  @POST
  @Path("rename")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "rename-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "rename-view", absolute = true)
  public Response renameView(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      RenameTableRequest request) {
    icebergTableOpsManager.getOps(prefix).renameView(request);
    return IcebergRestUtils.noContent();
  }
}
