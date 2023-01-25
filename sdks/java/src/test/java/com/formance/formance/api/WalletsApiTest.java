package com.formance.formance.api;

import com.formance.formance.ApiClient;
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
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for WalletsApi
 */
public class WalletsApiTest {

    private WalletsApi api;

    @Before
    public void setup() {
        api = new ApiClient().createService(WalletsApi.class);
    }

    /**
     * Confirm a hold
     *
     * 
     */
    @Test
    public void confirmHoldTest() {
        String holdId = null;
        ConfirmHoldRequest confirmHoldRequest = null;
        // api.confirmHold(holdId, confirmHoldRequest);

        // TODO: test validations
    }
    /**
     * Create a balance
     *
     * 
     */
    @Test
    public void createBalanceTest() {
        String id = null;
        Balance body = null;
        // CreateBalanceResponse response = api.createBalance(id, body);

        // TODO: test validations
    }
    /**
     * Create a new wallet
     *
     * 
     */
    @Test
    public void createWalletTest() {
        CreateWalletRequest createWalletRequest = null;
        // CreateWalletResponse response = api.createWallet(createWalletRequest);

        // TODO: test validations
    }
    /**
     * Credit a wallet
     *
     * 
     */
    @Test
    public void creditWalletTest() {
        String id = null;
        CreditWalletRequest creditWalletRequest = null;
        // api.creditWallet(id, creditWalletRequest);

        // TODO: test validations
    }
    /**
     * Debit a wallet
     *
     * 
     */
    @Test
    public void debitWalletTest() {
        String id = null;
        DebitWalletRequest debitWalletRequest = null;
        // DebitWalletResponse response = api.debitWallet(id, debitWalletRequest);

        // TODO: test validations
    }
    /**
     * Get detailed balance
     *
     * 
     */
    @Test
    public void getBalanceTest() {
        String id = null;
        String balanceName = null;
        // GetBalanceResponse response = api.getBalance(id, balanceName);

        // TODO: test validations
    }
    /**
     * Get a hold
     *
     * 
     */
    @Test
    public void getHoldTest() {
        String holdID = null;
        // GetHoldResponse response = api.getHold(holdID);

        // TODO: test validations
    }
    /**
     * Get all holds for a wallet
     *
     * 
     */
    @Test
    public void getHoldsTest() {
        Integer pageSize = null;
        String walletID = null;
        Object metadata = null;
        String cursor = null;
        // GetHoldsResponse response = api.getHolds(pageSize, walletID, metadata, cursor);

        // TODO: test validations
    }
    /**
     * 
     *
     * 
     */
    @Test
    public void getTransactionsTest() {
        Integer pageSize = null;
        String walletId = null;
        String cursor = null;
        // GetTransactionsResponse response = api.getTransactions(pageSize, walletId, cursor);

        // TODO: test validations
    }
    /**
     * Get a wallet
     *
     * 
     */
    @Test
    public void getWalletTest() {
        String id = null;
        // GetWalletResponse response = api.getWallet(id);

        // TODO: test validations
    }
    /**
     * List balances of a wallet
     *
     * 
     */
    @Test
    public void listBalancesTest() {
        String id = null;
        // ListBalancesResponse response = api.listBalances(id);

        // TODO: test validations
    }
    /**
     * List all wallets
     *
     * 
     */
    @Test
    public void listWalletsTest() {
        String name = null;
        Object metadata = null;
        Integer pageSize = null;
        String cursor = null;
        // ListWalletsResponse response = api.listWallets(name, metadata, pageSize, cursor);

        // TODO: test validations
    }
    /**
     * Update a wallet
     *
     * 
     */
    @Test
    public void updateWalletTest() {
        String id = null;
        UpdateWalletRequest updateWalletRequest = null;
        // api.updateWallet(id, updateWalletRequest);

        // TODO: test validations
    }
    /**
     * Cancel a hold
     *
     * 
     */
    @Test
    public void voidHoldTest() {
        String holdId = null;
        // api.voidHold(holdId);

        // TODO: test validations
    }
    /**
     * Get server info
     *
     * 
     */
    @Test
    public void walletsgetServerInfoTest() {
        // ServerInfo response = api.walletsgetServerInfo();

        // TODO: test validations
    }
}
