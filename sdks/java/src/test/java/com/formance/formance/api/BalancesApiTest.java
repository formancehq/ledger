package com.formance.formance.api;

import com.formance.formance.ApiClient;
import com.formance.formance.model.AggregateBalancesResponse;
import com.formance.formance.model.BalancesCursorResponse;
import com.formance.formance.model.ErrorResponse;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for BalancesApi
 */
public class BalancesApiTest {

    private BalancesApi api;

    @Before
    public void setup() {
        api = new ApiClient().createService(BalancesApi.class);
    }

    /**
     * Get the balances from a ledger&#39;s account
     *
     * 
     */
    @Test
    public void getBalancesTest() {
        String ledger = null;
        String address = null;
        String after = null;
        String cursor = null;
        String paginationToken = null;
        // BalancesCursorResponse response = api.getBalances(ledger, address, after, cursor, paginationToken);

        // TODO: test validations
    }
    /**
     * Get the aggregated balances from selected accounts
     *
     * 
     */
    @Test
    public void getBalancesAggregatedTest() {
        String ledger = null;
        String address = null;
        // AggregateBalancesResponse response = api.getBalancesAggregated(ledger, address);

        // TODO: test validations
    }
}
