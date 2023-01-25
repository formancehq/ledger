package com.formance.formance.api;

import com.formance.formance.ApiClient;
import com.formance.formance.model.ErrorResponse;
import com.formance.formance.model.StatsResponse;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for StatsApi
 */
public class StatsApiTest {

    private StatsApi api;

    @Before
    public void setup() {
        api = new ApiClient().createService(StatsApi.class);
    }

    /**
     * Get statistics from a ledger
     *
     * Get statistics from a ledger. (aggregate metrics on accounts and transactions) 
     */
    @Test
    public void readStatsTest() {
        String ledger = null;
        // StatsResponse response = api.readStats(ledger);

        // TODO: test validations
    }
}
