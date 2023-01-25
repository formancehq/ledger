package com.formance.formance.api;

import com.formance.formance.ApiClient;
import com.formance.formance.model.AttemptResponse;
import com.formance.formance.model.ConfigChangeSecret;
import com.formance.formance.model.ConfigResponse;
import com.formance.formance.model.ConfigUser;
import com.formance.formance.model.ConfigsResponse;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for WebhooksApi
 */
public class WebhooksApiTest {

    private WebhooksApi api;

    @Before
    public void setup() {
        api = new ApiClient().createService(WebhooksApi.class);
    }

    /**
     * Activate one config
     *
     * Activate a webhooks config by ID, to start receiving webhooks to its endpoint.
     */
    @Test
    public void activateConfigTest() {
        String id = null;
        // ConfigResponse response = api.activateConfig(id);

        // TODO: test validations
    }
    /**
     * Change the signing secret of a config
     *
     * Change the signing secret of the endpoint of a webhooks config.  If not passed or empty, a secret is automatically generated. The format is a random string of bytes of size 24, base64 encoded. (larger size after encoding) 
     */
    @Test
    public void changeConfigSecretTest() {
        String id = null;
        ConfigChangeSecret configChangeSecret = null;
        // ConfigResponse response = api.changeConfigSecret(id, configChangeSecret);

        // TODO: test validations
    }
    /**
     * Deactivate one config
     *
     * Deactivate a webhooks config by ID, to stop receiving webhooks to its endpoint.
     */
    @Test
    public void deactivateConfigTest() {
        String id = null;
        // ConfigResponse response = api.deactivateConfig(id);

        // TODO: test validations
    }
    /**
     * Delete one config
     *
     * Delete a webhooks config by ID.
     */
    @Test
    public void deleteConfigTest() {
        String id = null;
        // api.deleteConfig(id);

        // TODO: test validations
    }
    /**
     * Get many configs
     *
     * Sorted by updated date descending
     */
    @Test
    public void getManyConfigsTest() {
        String id = null;
        String endpoint = null;
        // ConfigsResponse response = api.getManyConfigs(id, endpoint);

        // TODO: test validations
    }
    /**
     * Insert a new config
     *
     * Insert a new webhooks config.  The endpoint should be a valid https URL and be unique.  The secret is the endpoint&#39;s verification secret. If not passed or empty, a secret is automatically generated. The format is a random string of bytes of size 24, base64 encoded. (larger size after encoding)  All eventTypes are converted to lower-case when inserted. 
     */
    @Test
    public void insertConfigTest() {
        ConfigUser configUser = null;
        // ConfigResponse response = api.insertConfig(configUser);

        // TODO: test validations
    }
    /**
     * Test one config
     *
     * Test a config by sending a webhook to its endpoint.
     */
    @Test
    public void testConfigTest() {
        String id = null;
        // AttemptResponse response = api.testConfig(id);

        // TODO: test validations
    }
}
