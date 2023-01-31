package com.formance.formance.api;

import com.formance.formance.CollectionFormats.*;

import retrofit2.Call;
import retrofit2.http.*;

import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okhttp3.MultipartBody;

import com.formance.formance.model.Balance;
import com.formance.formance.model.ConfirmHoldRequest;
import com.formance.formance.model.CreateBalanceResponse;
import com.formance.formance.model.CreateWalletRequest;
import com.formance.formance.model.CreateWalletResponse;
import com.formance.formance.model.CreditWalletRequest;
import com.formance.formance.model.DebitWalletRequest;
import com.formance.formance.model.DebitWalletResponse;
import com.formance.formance.model.GetBalanceResponse;
import com.formance.formance.model.GetHoldResponse;
import com.formance.formance.model.GetHoldsResponse;
import com.formance.formance.model.GetTransactionsResponse;
import com.formance.formance.model.GetWalletResponse;
import com.formance.formance.model.ListBalancesResponse;
import com.formance.formance.model.ListWalletsResponse;
import com.formance.formance.model.ServerInfo;
import com.formance.formance.model.UpdateWalletRequest;
import com.formance.formance.model.WalletsErrorResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface WalletsApi {
  /**
   * Confirm a hold
   * 
   * @param holdId  (required)
   * @param confirmHoldRequest  (optional)
   * @return Call&lt;Void&gt;
   */
  @Headers({
    "Content-Type:application/json"
  })
  @POST("api/wallets/holds/{hold_id}/confirm")
  Call<Void> confirmHold(
    @retrofit2.http.Path("hold_id") String holdId, @retrofit2.http.Body ConfirmHoldRequest confirmHoldRequest
  );

  /**
   * Create a balance
   * 
   * @param id  (required)
   * @param body  (optional)
   * @return Call&lt;CreateBalanceResponse&gt;
   */
  @Headers({
    "Content-Type:application/json"
  })
  @POST("api/wallets/wallets/{id}/balances")
  Call<CreateBalanceResponse> createBalance(
    @retrofit2.http.Path("id") String id, @retrofit2.http.Body Balance body
  );

  /**
   * Create a new wallet
   * 
   * @param createWalletRequest  (optional)
   * @return Call&lt;CreateWalletResponse&gt;
   */
  @Headers({
    "Content-Type:application/json"
  })
  @POST("api/wallets/wallets")
  Call<CreateWalletResponse> createWallet(
    @retrofit2.http.Body CreateWalletRequest createWalletRequest
  );

  /**
   * Credit a wallet
   * 
   * @param id  (required)
   * @param creditWalletRequest  (optional)
   * @return Call&lt;Void&gt;
   */
  @Headers({
    "Content-Type:application/json"
  })
  @POST("api/wallets/wallets/{id}/credit")
  Call<Void> creditWallet(
    @retrofit2.http.Path("id") String id, @retrofit2.http.Body CreditWalletRequest creditWalletRequest
  );

  /**
   * Debit a wallet
   * 
   * @param id  (required)
   * @param debitWalletRequest  (optional)
   * @return Call&lt;DebitWalletResponse&gt;
   */
  @Headers({
    "Content-Type:application/json"
  })
  @POST("api/wallets/wallets/{id}/debit")
  Call<DebitWalletResponse> debitWallet(
    @retrofit2.http.Path("id") String id, @retrofit2.http.Body DebitWalletRequest debitWalletRequest
  );

  /**
   * Get detailed balance
   * 
   * @param id  (required)
   * @param balanceName  (required)
   * @return Call&lt;GetBalanceResponse&gt;
   */
  @GET("api/wallets/wallets/{id}/balances/{balanceName}")
  Call<GetBalanceResponse> getBalance(
    @retrofit2.http.Path("id") String id, @retrofit2.http.Path("balanceName") String balanceName
  );

  /**
   * Get a hold
   * 
   * @param holdID The hold ID (required)
   * @return Call&lt;GetHoldResponse&gt;
   */
  @GET("api/wallets/holds/{holdID}")
  Call<GetHoldResponse> getHold(
    @retrofit2.http.Path("holdID") String holdID
  );

  /**
   * Get all holds for a wallet
   * 
   * @param pageSize The maximum number of results to return per page (optional, default to 15)
   * @param walletID The wallet to filter on (optional)
   * @param metadata Filter holds by metadata key value pairs. Nested objects can be used as seen in the example below. (optional)
   * @param cursor Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set.  (optional)
   * @return Call&lt;GetHoldsResponse&gt;
   */
  @GET("api/wallets/holds")
  Call<GetHoldsResponse> getHolds(
    @retrofit2.http.Query("pageSize") Integer pageSize, @retrofit2.http.Query("walletID") String walletID, @retrofit2.http.Query("metadata") Object metadata, @retrofit2.http.Query("cursor") String cursor
  );

  /**
   * 
   * 
   * @param pageSize The maximum number of results to return per page (optional, default to 15)
   * @param walletId A wallet ID to filter on (optional)
   * @param cursor Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the cursor is set.  (optional)
   * @return Call&lt;GetTransactionsResponse&gt;
   */
  @GET("api/wallets/transactions")
  Call<GetTransactionsResponse> getTransactions(
    @retrofit2.http.Query("pageSize") Integer pageSize, @retrofit2.http.Query("wallet_id") String walletId, @retrofit2.http.Query("cursor") String cursor
  );

  /**
   * Get a wallet
   * 
   * @param id  (required)
   * @return Call&lt;GetWalletResponse&gt;
   */
  @GET("api/wallets/wallets/{id}")
  Call<GetWalletResponse> getWallet(
    @retrofit2.http.Path("id") String id
  );

  /**
   * List balances of a wallet
   * 
   * @param id  (required)
   * @return Call&lt;ListBalancesResponse&gt;
   */
  @GET("api/wallets/wallets/{id}/balances")
  Call<ListBalancesResponse> listBalances(
    @retrofit2.http.Path("id") String id
  );

  /**
   * List all wallets
   * 
   * @param name Filter on wallet name (optional)
   * @param metadata Filter wallets by metadata key value pairs. Nested objects can be used as seen in the example below. (optional)
   * @param pageSize The maximum number of results to return per page (optional, default to 15)
   * @param cursor Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set.  (optional)
   * @return Call&lt;ListWalletsResponse&gt;
   */
  @GET("api/wallets/wallets")
  Call<ListWalletsResponse> listWallets(
    @retrofit2.http.Query("name") String name, @retrofit2.http.Query("metadata") Object metadata, @retrofit2.http.Query("pageSize") Integer pageSize, @retrofit2.http.Query("cursor") String cursor
  );

  /**
   * Update a wallet
   * 
   * @param id  (required)
   * @param updateWalletRequest  (optional)
   * @return Call&lt;Void&gt;
   */
  @Headers({
    "Content-Type:application/json"
  })
  @PATCH("api/wallets/wallets/{id}")
  Call<Void> updateWallet(
    @retrofit2.http.Path("id") String id, @retrofit2.http.Body UpdateWalletRequest updateWalletRequest
  );

  /**
   * Cancel a hold
   * 
   * @param holdId  (required)
   * @return Call&lt;Void&gt;
   */
  @POST("api/wallets/holds/{hold_id}/void")
  Call<Void> voidHold(
    @retrofit2.http.Path("hold_id") String holdId
  );

  /**
   * Get server info
   * 
   * @return Call&lt;ServerInfo&gt;
   */
  @GET("api/wallets/_info")
  Call<ServerInfo> walletsgetServerInfo();
    

}
