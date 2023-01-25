package com.formance.formance.api;

import com.formance.formance.ApiClient;
import com.formance.formance.model.ErrorResponse;
import com.formance.formance.model.LedgerInfoResponse;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for LedgerApi
 */
public class LedgerApiTest {

    private LedgerApi api;

    @Before
    public void setup() {
        api = new ApiClient().createService(LedgerApi.class);
    }

    /**
     * Get information about a ledger
     *
     * 
     */
    @Test
    public void getLedgerInfoTest() {
        String ledger = null;
        // LedgerInfoResponse response = api.getLedgerInfo(ledger);

        // TODO: test validations
    }
}
