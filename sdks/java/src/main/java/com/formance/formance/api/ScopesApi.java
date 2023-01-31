package com.formance.formance.api;

import com.formance.formance.CollectionFormats.*;

import retrofit2.Call;
import retrofit2.http.*;

import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okhttp3.MultipartBody;

import com.formance.formance.model.CreateScopeResponse;
import com.formance.formance.model.ListScopesResponse;
import com.formance.formance.model.ScopeOptions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ScopesApi {
  /**
   * Add a transient scope to a scope
   * Add a transient scope to a scope
   * @param scopeId Scope ID (required)
   * @param transientScopeId Transient scope ID (required)
   * @return Call&lt;Void&gt;
   */
  @PUT("api/auth/scopes/{scopeId}/transient/{transientScopeId}")
  Call<Void> addTransientScope(
    @retrofit2.http.Path("scopeId") String scopeId, @retrofit2.http.Path("transientScopeId") String transientScopeId
  );

  /**
   * Create scope
   * Create scope
   * @param body  (optional)
   * @return Call&lt;CreateScopeResponse&gt;
   */
  @Headers({
    "Content-Type:application/json"
  })
  @POST("api/auth/scopes")
  Call<CreateScopeResponse> createScope(
    @retrofit2.http.Body ScopeOptions body
  );

  /**
   * Delete scope
   * Delete scope
   * @param scopeId Scope ID (required)
   * @return Call&lt;Void&gt;
   */
  @DELETE("api/auth/scopes/{scopeId}")
  Call<Void> deleteScope(
    @retrofit2.http.Path("scopeId") String scopeId
  );

  /**
   * Delete a transient scope from a scope
   * Delete a transient scope from a scope
   * @param scopeId Scope ID (required)
   * @param transientScopeId Transient scope ID (required)
   * @return Call&lt;Void&gt;
   */
  @DELETE("api/auth/scopes/{scopeId}/transient/{transientScopeId}")
  Call<Void> deleteTransientScope(
    @retrofit2.http.Path("scopeId") String scopeId, @retrofit2.http.Path("transientScopeId") String transientScopeId
  );

  /**
   * List scopes
   * List Scopes
   * @return Call&lt;ListScopesResponse&gt;
   */
  @GET("api/auth/scopes")
  Call<ListScopesResponse> listScopes();
    

  /**
   * Read scope
   * Read scope
   * @param scopeId Scope ID (required)
   * @return Call&lt;CreateScopeResponse&gt;
   */
  @GET("api/auth/scopes/{scopeId}")
  Call<CreateScopeResponse> readScope(
    @retrofit2.http.Path("scopeId") String scopeId
  );

  /**
   * Update scope
   * Update scope
   * @param scopeId Scope ID (required)
   * @param body  (optional)
   * @return Call&lt;CreateScopeResponse&gt;
   */
  @Headers({
    "Content-Type:application/json"
  })
  @PUT("api/auth/scopes/{scopeId}")
  Call<CreateScopeResponse> updateScope(
    @retrofit2.http.Path("scopeId") String scopeId, @retrofit2.http.Body ScopeOptions body
  );

}
