package com.formance.formance.api;

import com.formance.formance.CollectionFormats.*;

import retrofit2.Call;
import retrofit2.http.*;

import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okhttp3.MultipartBody;

import com.formance.formance.model.CreateWorkflowResponse;
import com.formance.formance.model.Error;
import com.formance.formance.model.GetWorkflowOccurrenceResponse;
import com.formance.formance.model.GetWorkflowResponse;
import com.formance.formance.model.ListRunsResponse;
import com.formance.formance.model.ListWorkflowsResponse;
import com.formance.formance.model.RunWorkflowResponse;
import com.formance.formance.model.ServerInfo;
import com.formance.formance.model.WorkflowConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface OrchestrationApi {
  /**
   * Create workflow
   * Create a workflow
   * @param body  (optional)
   * @return Call&lt;CreateWorkflowResponse&gt;
   */
  @Headers({
    "Content-Type:application/json"
  })
  @POST("api/orchestration/flows")
  Call<CreateWorkflowResponse> createWorkflow(
    @retrofit2.http.Body WorkflowConfig body
  );

  /**
   * Get a flow by id
   * Get a flow by id
   * @param flowId The flow id (required)
   * @return Call&lt;GetWorkflowResponse&gt;
   */
  @GET("api/orchestration/flows/{flowId}")
  Call<GetWorkflowResponse> getFlow(
    @retrofit2.http.Path("flowId") String flowId
  );

  /**
   * Get a workflow occurrence by id
   * Get a workflow occurrence by id
   * @param flowId The flow id (required)
   * @param runId The occurrence id (required)
   * @return Call&lt;GetWorkflowOccurrenceResponse&gt;
   */
  @GET("api/orchestration/flows/{flowId}/runs/{runId}")
  Call<GetWorkflowOccurrenceResponse> getWorkflowOccurrence(
    @retrofit2.http.Path("flowId") String flowId, @retrofit2.http.Path("runId") String runId
  );

  /**
   * List registered flows
   * List registered flows
   * @return Call&lt;ListWorkflowsResponse&gt;
   */
  @GET("api/orchestration/flows")
  Call<ListWorkflowsResponse> listFlows();
    

  /**
   * List occurrences of a workflow
   * List occurrences of a workflow
   * @param flowId The flow id (required)
   * @return Call&lt;ListRunsResponse&gt;
   */
  @GET("api/orchestration/flows/{flowId}/runs")
  Call<ListRunsResponse> listRuns(
    @retrofit2.http.Path("flowId") String flowId
  );

  /**
   * Get server info
   * 
   * @return Call&lt;ServerInfo&gt;
   */
  @GET("api/orchestration/_info")
  Call<ServerInfo> orchestrationgetServerInfo();
    

  /**
   * Run workflow
   * Run workflow
   * @param flowId The flow id (required)
   * @param wait Wait end of the workflow before return (optional)
   * @param requestBody  (optional)
   * @return Call&lt;RunWorkflowResponse&gt;
   */
  @Headers({
    "Content-Type:application/json"
  })
  @POST("api/orchestration/flows/{flowId}/runs")
  Call<RunWorkflowResponse> runWorkflow(
    @retrofit2.http.Path("flowId") String flowId, @retrofit2.http.Query("wait") Boolean wait, @retrofit2.http.Body Map<String, String> requestBody
  );

}
