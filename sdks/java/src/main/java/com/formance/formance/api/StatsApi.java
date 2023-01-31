package com.formance.formance.api;

import com.formance.formance.CollectionFormats.*;

import retrofit2.Call;
import retrofit2.http.*;

import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okhttp3.MultipartBody;

import com.formance.formance.model.ErrorResponse;
import com.formance.formance.model.StatsResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface StatsApi {
  /**
   * Get statistics from a ledger
   * Get statistics from a ledger. (aggregate metrics on accounts and transactions) 
   * @param ledger name of the ledger (required)
   * @return Call&lt;StatsResponse&gt;
   */
  @GET("api/ledger/{ledger}/stats")
  Call<StatsResponse> readStats(
    @retrofit2.http.Path("ledger") String ledger
  );

}
