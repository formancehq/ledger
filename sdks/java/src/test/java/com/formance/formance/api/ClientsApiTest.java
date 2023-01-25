package com.formance.formance.api;

import com.formance.formance.ApiClient;
import com.formance.formance.model.ClientOptions;
import com.formance.formance.model.CreateClientResponse;
import com.formance.formance.model.CreateSecretResponse;
import com.formance.formance.model.ListClientsResponse;
import com.formance.formance.model.ReadClientResponse;
import com.formance.formance.model.SecretOptions;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for ClientsApi
 */
public class ClientsApiTest {

    private ClientsApi api;

    @Before
    public void setup() {
        api = new ApiClient().createService(ClientsApi.class);
    }

    /**
     * Add scope to client
     *
     * 
     */
    @Test
    public void addScopeToClientTest() {
        String clientId = null;
        String scopeId = null;
        // api.addScopeToClient(clientId, scopeId);

        // TODO: test validations
    }
    /**
     * Create client
     *
     * 
     */
    @Test
    public void createClientTest() {
        ClientOptions body = null;
        // CreateClientResponse response = api.createClient(body);

        // TODO: test validations
    }
    /**
     * Add a secret to a client
     *
     * 
     */
    @Test
    public void createSecretTest() {
        String clientId = null;
        SecretOptions body = null;
        // CreateSecretResponse response = api.createSecret(clientId, body);

        // TODO: test validations
    }
    /**
     * Delete client
     *
     * 
     */
    @Test
    public void deleteClientTest() {
        String clientId = null;
        // api.deleteClient(clientId);

        // TODO: test validations
    }
    /**
     * Delete scope from client
     *
     * 
     */
    @Test
    public void deleteScopeFromClientTest() {
        String clientId = null;
        String scopeId = null;
        // api.deleteScopeFromClient(clientId, scopeId);

        // TODO: test validations
    }
    /**
     * Delete a secret from a client
     *
     * 
     */
    @Test
    public void deleteSecretTest() {
        String clientId = null;
        String secretId = null;
        // api.deleteSecret(clientId, secretId);

        // TODO: test validations
    }
    /**
     * List clients
     *
     * 
     */
    @Test
    public void listClientsTest() {
        // ListClientsResponse response = api.listClients();

        // TODO: test validations
    }
    /**
     * Read client
     *
     * 
     */
    @Test
    public void readClientTest() {
        String clientId = null;
        // ReadClientResponse response = api.readClient(clientId);

        // TODO: test validations
    }
    /**
     * Update client
     *
     * 
     */
    @Test
    public void updateClientTest() {
        String clientId = null;
        ClientOptions body = null;
        // CreateClientResponse response = api.updateClient(clientId, body);

        // TODO: test validations
    }
}
