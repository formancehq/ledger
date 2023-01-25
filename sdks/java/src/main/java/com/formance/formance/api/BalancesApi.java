package com.formance.formance.api;

import com.formance.formance.CollectionFormats.*;

import retrofit2.Call;
import retrofit2.http.*;

import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okhttp3.MultipartBody;

import com.formance.formance.model.AggregateBalancesResponse;
import com.formance.formance.model.BalancesCursorResponse;
import com.formance.formance.model.ErrorResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface BalancesApi {
  /**
   * Get the balances from a ledger&#39;s account
   * 
   * @param ledger Name of the ledger. (required)
   * @param address Filter balances involving given account, either as source or destination. (optional)
   * @param after Pagination cursor, will return accounts after given address, in descending order. (optional)
   * @param cursor Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  (optional)
   * @param paginationToken Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. Deprecated, please use &#x60;cursor&#x60; instead. (optional)
   * @return Call&lt;BalancesCursorResponse&gt;
   */
  @GET("api/ledger/{ledger}/balances")
  Call<BalancesCursorResponse> getBalances(
    @retrofit2.http.Path("ledger") String ledger, @retrofit2.http.Query("address") String address, @retrofit2.http.Query("after") String after, @retrofit2.http.Query("cursor") String cursor, @retrofit2.http.Query("pagination_token") String paginationToken
  );

  /**
   * Get the aggregated balances from selected accounts
   * 
   * @param ledger Name of the ledger. (required)
   * @param address Filter balances involving given account, either as source or destination. (optional)
   * @return Call&lt;AggregateBalancesResponse&gt;
   */
  @GET("api/ledger/{ledger}/aggregate/balances")
  Call<AggregateBalancesResponse> getBalancesAggregated(
    @retrofit2.http.Path("ledger") String ledger, @retrofit2.http.Query("address") String address
  );

}
