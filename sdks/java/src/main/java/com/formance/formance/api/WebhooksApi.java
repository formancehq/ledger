package com.formance.formance.api;

import com.formance.formance.CollectionFormats.*;

import retrofit2.Call;
import retrofit2.http.*;

import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okhttp3.MultipartBody;

import com.formance.formance.model.AttemptResponse;
import com.formance.formance.model.ConfigChangeSecret;
import com.formance.formance.model.ConfigResponse;
import com.formance.formance.model.ConfigUser;
import com.formance.formance.model.ConfigsResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface WebhooksApi {
  /**
   * Activate one config
   * Activate a webhooks config by ID, to start receiving webhooks to its endpoint.
   * @param id Config ID (required)
   * @return Call&lt;ConfigResponse&gt;
   */
  @PUT("api/webhooks/configs/{id}/activate")
  Call<ConfigResponse> activateConfig(
    @retrofit2.http.Path("id") String id
  );

  /**
   * Change the signing secret of a config
   * Change the signing secret of the endpoint of a webhooks config.  If not passed or empty, a secret is automatically generated. The format is a random string of bytes of size 24, base64 encoded. (larger size after encoding) 
   * @param id Config ID (required)
   * @param configChangeSecret  (optional)
   * @return Call&lt;ConfigResponse&gt;
   */
  @Headers({
    "Content-Type:application/json"
  })
  @PUT("api/webhooks/configs/{id}/secret/change")
  Call<ConfigResponse> changeConfigSecret(
    @retrofit2.http.Path("id") String id, @retrofit2.http.Body ConfigChangeSecret configChangeSecret
  );

  /**
   * Deactivate one config
   * Deactivate a webhooks config by ID, to stop receiving webhooks to its endpoint.
   * @param id Config ID (required)
   * @return Call&lt;ConfigResponse&gt;
   */
  @PUT("api/webhooks/configs/{id}/deactivate")
  Call<ConfigResponse> deactivateConfig(
    @retrofit2.http.Path("id") String id
  );

  /**
   * Delete one config
   * Delete a webhooks config by ID.
   * @param id Config ID (required)
   * @return Call&lt;Void&gt;
   */
  @DELETE("api/webhooks/configs/{id}")
  Call<Void> deleteConfig(
    @retrofit2.http.Path("id") String id
  );

  /**
   * Get many configs
   * Sorted by updated date descending
   * @param id Optional filter by Config ID (optional)
   * @param endpoint Optional filter by endpoint URL (optional)
   * @return Call&lt;ConfigsResponse&gt;
   */
  @GET("api/webhooks/configs")
  Call<ConfigsResponse> getManyConfigs(
    @retrofit2.http.Query("id") String id, @retrofit2.http.Query("endpoint") String endpoint
  );

  /**
   * Insert a new config
   * Insert a new webhooks config.  The endpoint should be a valid https URL and be unique.  The secret is the endpoint&#39;s verification secret. If not passed or empty, a secret is automatically generated. The format is a random string of bytes of size 24, base64 encoded. (larger size after encoding)  All eventTypes are converted to lower-case when inserted. 
   * @param configUser  (required)
   * @return Call&lt;ConfigResponse&gt;
   */
  @Headers({
    "Content-Type:application/json"
  })
  @POST("api/webhooks/configs")
  Call<ConfigResponse> insertConfig(
    @retrofit2.http.Body ConfigUser configUser
  );

  /**
   * Test one config
   * Test a config by sending a webhook to its endpoint.
   * @param id Config ID (required)
   * @return Call&lt;AttemptResponse&gt;
   */
  @GET("api/webhooks/configs/{id}/test")
  Call<AttemptResponse> testConfig(
    @retrofit2.http.Path("id") String id
  );

}
