package com.formance.formance.api;

import com.formance.formance.CollectionFormats.*;

import retrofit2.Call;
import retrofit2.http.*;

import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okhttp3.MultipartBody;

import com.formance.formance.model.ClientOptions;
import com.formance.formance.model.CreateClientResponse;
import com.formance.formance.model.CreateSecretResponse;
import com.formance.formance.model.ListClientsResponse;
import com.formance.formance.model.ReadClientResponse;
import com.formance.formance.model.SecretOptions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface ClientsApi {
  /**
   * Add scope to client
   * 
   * @param clientId Client ID (required)
   * @param scopeId Scope ID (required)
   * @return Call&lt;Void&gt;
   */
  @PUT("api/auth/clients/{clientId}/scopes/{scopeId}")
  Call<Void> addScopeToClient(
    @retrofit2.http.Path("clientId") String clientId, @retrofit2.http.Path("scopeId") String scopeId
  );

  /**
   * Create client
   * 
   * @param body  (optional)
   * @return Call&lt;CreateClientResponse&gt;
   */
  @Headers({
    "Content-Type:application/json"
  })
  @POST("api/auth/clients")
  Call<CreateClientResponse> createClient(
    @retrofit2.http.Body ClientOptions body
  );

  /**
   * Add a secret to a client
   * 
   * @param clientId Client ID (required)
   * @param body  (optional)
   * @return Call&lt;CreateSecretResponse&gt;
   */
  @Headers({
    "Content-Type:application/json"
  })
  @POST("api/auth/clients/{clientId}/secrets")
  Call<CreateSecretResponse> createSecret(
    @retrofit2.http.Path("clientId") String clientId, @retrofit2.http.Body SecretOptions body
  );

  /**
   * Delete client
   * 
   * @param clientId Client ID (required)
   * @return Call&lt;Void&gt;
   */
  @DELETE("api/auth/clients/{clientId}")
  Call<Void> deleteClient(
    @retrofit2.http.Path("clientId") String clientId
  );

  /**
   * Delete scope from client
   * 
   * @param clientId Client ID (required)
   * @param scopeId Scope ID (required)
   * @return Call&lt;Void&gt;
   */
  @DELETE("api/auth/clients/{clientId}/scopes/{scopeId}")
  Call<Void> deleteScopeFromClient(
    @retrofit2.http.Path("clientId") String clientId, @retrofit2.http.Path("scopeId") String scopeId
  );

  /**
   * Delete a secret from a client
   * 
   * @param clientId Client ID (required)
   * @param secretId Secret ID (required)
   * @return Call&lt;Void&gt;
   */
  @DELETE("api/auth/clients/{clientId}/secrets/{secretId}")
  Call<Void> deleteSecret(
    @retrofit2.http.Path("clientId") String clientId, @retrofit2.http.Path("secretId") String secretId
  );

  /**
   * List clients
   * 
   * @return Call&lt;ListClientsResponse&gt;
   */
  @GET("api/auth/clients")
  Call<ListClientsResponse> listClients();
    

  /**
   * Read client
   * 
   * @param clientId Client ID (required)
   * @return Call&lt;ReadClientResponse&gt;
   */
  @GET("api/auth/clients/{clientId}")
  Call<ReadClientResponse> readClient(
    @retrofit2.http.Path("clientId") String clientId
  );

  /**
   * Update client
   * 
   * @param clientId Client ID (required)
   * @param body  (optional)
   * @return Call&lt;CreateClientResponse&gt;
   */
  @Headers({
    "Content-Type:application/json"
  })
  @PUT("api/auth/clients/{clientId}")
  Call<CreateClientResponse> updateClient(
    @retrofit2.http.Path("clientId") String clientId, @retrofit2.http.Body ClientOptions body
  );

}
