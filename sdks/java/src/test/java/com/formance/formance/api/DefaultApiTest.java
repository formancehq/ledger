package com.formance.formance.api;

import com.formance.formance.ApiClient;
import com.formance.formance.model.ServerInfo;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for DefaultApi
 */
public class DefaultApiTest {

    private DefaultApi api;

    @Before
    public void setup() {
        api = new ApiClient().createService(DefaultApi.class);
    }

    /**
     * Get server info
     *
     * 
     */
    @Test
    public void getServerInfoTest() {
        // ServerInfo response = api.getServerInfo();

        // TODO: test validations
    }
    /**
     * Get server info
     *
     * 
     */
    @Test
    public void searchgetServerInfoTest() {
        // ServerInfo response = api.searchgetServerInfo();

        // TODO: test validations
    }
}
