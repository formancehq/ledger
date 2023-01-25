package com.formance.formance.api;

import com.formance.formance.ApiClient;
import com.formance.formance.model.ErrorResponse;
import com.formance.formance.model.Mapping;
import com.formance.formance.model.MappingResponse;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for MappingApi
 */
public class MappingApiTest {

    private MappingApi api;

    @Before
    public void setup() {
        api = new ApiClient().createService(MappingApi.class);
    }

    /**
     * Get the mapping of a ledger
     *
     * 
     */
    @Test
    public void getMappingTest() {
        String ledger = null;
        // MappingResponse response = api.getMapping(ledger);

        // TODO: test validations
    }
    /**
     * Update the mapping of a ledger
     *
     * 
     */
    @Test
    public void updateMappingTest() {
        String ledger = null;
        Mapping mapping = null;
        // MappingResponse response = api.updateMapping(ledger, mapping);

        // TODO: test validations
    }
}
