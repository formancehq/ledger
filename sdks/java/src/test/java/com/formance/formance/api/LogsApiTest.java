package com.formance.formance.api;

import com.formance.formance.ApiClient;
import com.formance.formance.model.ErrorResponse;
import com.formance.formance.model.LogsCursorResponse;
import java.time.OffsetDateTime;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for LogsApi
 */
public class LogsApiTest {

    private LogsApi api;

    @Before
    public void setup() {
        api = new ApiClient().createService(LogsApi.class);
    }

    /**
     * List the logs from a ledger
     *
     * List the logs from a ledger, sorted by ID in descending order.
     */
    @Test
    public void listLogsTest() {
        String ledger = null;
        Long pageSize = null;
        Long pageSize2 = null;
        String after = null;
        OffsetDateTime startTime = null;
        OffsetDateTime startTime2 = null;
        OffsetDateTime endTime = null;
        OffsetDateTime endTime2 = null;
        String cursor = null;
        String paginationToken = null;
        // LogsCursorResponse response = api.listLogs(ledger, pageSize, pageSize2, after, startTime, startTime2, endTime, endTime2, cursor, paginationToken);

        // TODO: test validations
    }
}
