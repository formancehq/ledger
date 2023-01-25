package com.formance.formance.api;

import com.formance.formance.ApiClient;
import com.formance.formance.model.CreateWorkflowResponse;
import com.formance.formance.model.Error;
import com.formance.formance.model.GetWorkflowOccurrenceResponse;
import com.formance.formance.model.GetWorkflowResponse;
import com.formance.formance.model.ListRunsResponse;
import com.formance.formance.model.ListWorkflowsResponse;
import com.formance.formance.model.RunWorkflowResponse;
import com.formance.formance.model.ServerInfo;
import com.formance.formance.model.WorkflowConfig;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for OrchestrationApi
 */
public class OrchestrationApiTest {

    private OrchestrationApi api;

    @Before
    public void setup() {
        api = new ApiClient().createService(OrchestrationApi.class);
    }

    /**
     * Create workflow
     *
     * Create a workflow
     */
    @Test
    public void createWorkflowTest() {
        WorkflowConfig body = null;
        // CreateWorkflowResponse response = api.createWorkflow(body);

        // TODO: test validations
    }
    /**
     * Get a flow by id
     *
     * Get a flow by id
     */
    @Test
    public void getFlowTest() {
        String flowId = null;
        // GetWorkflowResponse response = api.getFlow(flowId);

        // TODO: test validations
    }
    /**
     * Get a workflow occurrence by id
     *
     * Get a workflow occurrence by id
     */
    @Test
    public void getWorkflowOccurrenceTest() {
        String flowId = null;
        String runId = null;
        // GetWorkflowOccurrenceResponse response = api.getWorkflowOccurrence(flowId, runId);

        // TODO: test validations
    }
    /**
     * List registered flows
     *
     * List registered flows
     */
    @Test
    public void listFlowsTest() {
        // ListWorkflowsResponse response = api.listFlows();

        // TODO: test validations
    }
    /**
     * List occurrences of a workflow
     *
     * List occurrences of a workflow
     */
    @Test
    public void listRunsTest() {
        String flowId = null;
        // ListRunsResponse response = api.listRuns(flowId);

        // TODO: test validations
    }
    /**
     * Get server info
     *
     * 
     */
    @Test
    public void orchestrationgetServerInfoTest() {
        // ServerInfo response = api.orchestrationgetServerInfo();

        // TODO: test validations
    }
    /**
     * Run workflow
     *
     * Run workflow
     */
    @Test
    public void runWorkflowTest() {
        String flowId = null;
        Boolean wait = null;
        Map<String, String> requestBody = null;
        // RunWorkflowResponse response = api.runWorkflow(flowId, wait, requestBody);

        // TODO: test validations
    }
}
