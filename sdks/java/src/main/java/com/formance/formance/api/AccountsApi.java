package com.formance.formance.api;

import com.formance.formance.CollectionFormats.*;

import retrofit2.Call;
import retrofit2.http.*;

import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okhttp3.MultipartBody;

import com.formance.formance.model.AccountResponse;
import com.formance.formance.model.AccountsCursorResponse;
import com.formance.formance.model.ErrorResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface AccountsApi {
  /**
   * Add metadata to an account
   * 
   * @param ledger Name of the ledger. (required)
   * @param address Exact address of the account. It must match the following regular expressions pattern: &#x60;&#x60;&#x60; ^\\w+(:\\w+)*$ &#x60;&#x60;&#x60;  (required)
   * @param requestBody metadata (required)
   * @return Call&lt;Void&gt;
   */
  @Headers({
    "Content-Type:application/json"
  })
  @POST("api/ledger/{ledger}/accounts/{address}/metadata")
  Call<Void> addMetadataToAccount(
    @retrofit2.http.Path("ledger") String ledger, @retrofit2.http.Path("address") String address, @retrofit2.http.Body Map<String, Object> requestBody
  );

  /**
   * Count the accounts from a ledger
   * 
   * @param ledger Name of the ledger. (required)
   * @param address Filter accounts by address pattern (regular expression placed between ^ and $). (optional)
   * @param metadata Filter accounts by metadata key value pairs. Nested objects can be used as seen in the example below. (optional)
   * @return Call&lt;Void&gt;
   */
  @HEAD("api/ledger/{ledger}/accounts")
  Call<Void> countAccounts(
    @retrofit2.http.Path("ledger") String ledger, @retrofit2.http.Query("address") String address, @retrofit2.http.Query("metadata") Object metadata
  );

  /**
   * Get account by its address
   * 
   * @param ledger Name of the ledger. (required)
   * @param address Exact address of the account. It must match the following regular expressions pattern: &#x60;&#x60;&#x60; ^\\w+(:\\w+)*$ &#x60;&#x60;&#x60;  (required)
   * @return Call&lt;AccountResponse&gt;
   */
  @GET("api/ledger/{ledger}/accounts/{address}")
  Call<AccountResponse> getAccount(
    @retrofit2.http.Path("ledger") String ledger, @retrofit2.http.Path("address") String address
  );

  /**
   * List accounts from a ledger
   * List accounts from a ledger, sorted by address in descending order.
   * @param ledger Name of the ledger. (required)
   * @param pageSize The maximum number of results to return per page.  (optional, default to 15)
   * @param pageSize2 The maximum number of results to return per page. Deprecated, please use &#x60;pageSize&#x60; instead.  (optional, default to 15)
   * @param after Pagination cursor, will return accounts after given address, in descending order. (optional)
   * @param address Filter accounts by address pattern (regular expression placed between ^ and $). (optional)
   * @param metadata Filter accounts by metadata key value pairs. Nested objects can be used as seen in the example below. (optional)
   * @param balance Filter accounts by their balance (default operator is gte) (optional)
   * @param balanceOperator Operator used for the filtering of balances can be greater than/equal, less than/equal, greater than, less than, equal or not.  (optional)
   * @param balanceOperator2 Operator used for the filtering of balances can be greater than/equal, less than/equal, greater than, less than, equal or not. Deprecated, please use &#x60;balanceOperator&#x60; instead.  (optional)
   * @param cursor Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  (optional)
   * @param paginationToken Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use &#x60;cursor&#x60; instead.  (optional)
   * @return Call&lt;AccountsCursorResponse&gt;
   */
  @GET("api/ledger/{ledger}/accounts")
  Call<AccountsCursorResponse> listAccounts(
    @retrofit2.http.Path("ledger") String ledger, @retrofit2.http.Query("pageSize") Long pageSize, @retrofit2.http.Query("page_size") Long pageSize2, @retrofit2.http.Query("after") String after, @retrofit2.http.Query("address") String address, @retrofit2.http.Query("metadata") Object metadata, @retrofit2.http.Query("balance") Long balance, @retrofit2.http.Query("balanceOperator") String balanceOperator, @retrofit2.http.Query("balance_operator") String balanceOperator2, @retrofit2.http.Query("cursor") String cursor, @retrofit2.http.Query("pagination_token") String paginationToken
  );

}
