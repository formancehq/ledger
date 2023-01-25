package com.formance.formance.api;

import com.formance.formance.ApiClient;
import com.formance.formance.model.ConfigInfoResponse;
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
 * API tests for ServerApi
 */
public class ServerApiTest {

    private ServerApi api;

    @Before
    public void setup() {
        api = new ApiClient().createService(ServerApi.class);
    }

    /**
     * Show server information
     *
     * 
     */
    @Test
    public void getInfoTest() {
        // ConfigInfoResponse response = api.getInfo();

        // TODO: test validations
    }
}
