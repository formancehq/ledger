package com.formance.formance.api;

import com.formance.formance.ApiClient;
import com.formance.formance.model.Query;
import com.formance.formance.model.Response;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for SearchApi
 */
public class SearchApiTest {

    private SearchApi api;

    @Before
    public void setup() {
        api = new ApiClient().createService(SearchApi.class);
    }

    /**
     * Search
     *
     * ElasticSearch query engine
     */
    @Test
    public void searchTest() {
        Query query = null;
        // Response response = api.search(query);

        // TODO: test validations
    }
}
