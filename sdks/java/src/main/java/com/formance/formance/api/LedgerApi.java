package com.formance.formance.api;

import com.formance.formance.CollectionFormats.*;

import retrofit2.Call;
import retrofit2.http.*;

import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okhttp3.MultipartBody;

import com.formance.formance.model.ErrorResponse;
import com.formance.formance.model.LedgerInfoResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface LedgerApi {
  /**
   * Get information about a ledger
   * 
   * @param ledger Name of the ledger. (required)
   * @return Call&lt;LedgerInfoResponse&gt;
   */
  @GET("api/ledger/{ledger}/_info")
  Call<LedgerInfoResponse> getLedgerInfo(
    @retrofit2.http.Path("ledger") String ledger
  );

}
