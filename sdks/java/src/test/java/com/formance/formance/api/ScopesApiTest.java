package com.formance.formance.api;

import com.formance.formance.ApiClient;
import com.formance.formance.model.CreateScopeResponse;
import com.formance.formance.model.ListScopesResponse;
import com.formance.formance.model.ScopeOptions;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for ScopesApi
 */
public class ScopesApiTest {

    private ScopesApi api;

    @Before
    public void setup() {
        api = new ApiClient().createService(ScopesApi.class);
    }

    /**
     * Add a transient scope to a scope
     *
     * Add a transient scope to a scope
     */
    @Test
    public void addTransientScopeTest() {
        String scopeId = null;
        String transientScopeId = null;
        // api.addTransientScope(scopeId, transientScopeId);

        // TODO: test validations
    }
    /**
     * Create scope
     *
     * Create scope
     */
    @Test
    public void createScopeTest() {
        ScopeOptions body = null;
        // CreateScopeResponse response = api.createScope(body);

        // TODO: test validations
    }
    /**
     * Delete scope
     *
     * Delete scope
     */
    @Test
    public void deleteScopeTest() {
        String scopeId = null;
        // api.deleteScope(scopeId);

        // TODO: test validations
    }
    /**
     * Delete a transient scope from a scope
     *
     * Delete a transient scope from a scope
     */
    @Test
    public void deleteTransientScopeTest() {
        String scopeId = null;
        String transientScopeId = null;
        // api.deleteTransientScope(scopeId, transientScopeId);

        // TODO: test validations
    }
    /**
     * List scopes
     *
     * List Scopes
     */
    @Test
    public void listScopesTest() {
        // ListScopesResponse response = api.listScopes();

        // TODO: test validations
    }
    /**
     * Read scope
     *
     * Read scope
     */
    @Test
    public void readScopeTest() {
        String scopeId = null;
        // CreateScopeResponse response = api.readScope(scopeId);

        // TODO: test validations
    }
    /**
     * Update scope
     *
     * Update scope
     */
    @Test
    public void updateScopeTest() {
        String scopeId = null;
        ScopeOptions body = null;
        // CreateScopeResponse response = api.updateScope(scopeId, body);

        // TODO: test validations
    }
}
