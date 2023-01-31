package com.formance.formance.api;

import com.formance.formance.CollectionFormats.*;

import retrofit2.Call;
import retrofit2.http.*;

import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okhttp3.MultipartBody;

import com.formance.formance.model.Query;
import com.formance.formance.model.Response;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface SearchApi {
  /**
   * Search
   * ElasticSearch query engine
   * @param query  (required)
   * @return Call&lt;Response&gt;
   */
  @Headers({
    "Content-Type:application/json"
  })
  @POST("api/search/")
  Call<Response> search(
    @retrofit2.http.Body Query query
  );

}
