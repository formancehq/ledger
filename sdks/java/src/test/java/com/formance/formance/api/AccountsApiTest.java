package com.formance.formance.api;

import com.formance.formance.ApiClient;
import com.formance.formance.model.AccountResponse;
import com.formance.formance.model.AccountsCursorResponse;
import com.formance.formance.model.ErrorResponse;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for AccountsApi
 */
public class AccountsApiTest {

    private AccountsApi api;

    @Before
    public void setup() {
        api = new ApiClient().createService(AccountsApi.class);
    }

    /**
     * Add metadata to an account
     *
     * 
     */
    @Test
    public void addMetadataToAccountTest() {
        String ledger = null;
        String address = null;
        Map<String, Object> requestBody = null;
        // api.addMetadataToAccount(ledger, address, requestBody);

        // TODO: test validations
    }
    /**
     * Count the accounts from a ledger
     *
     * 
     */
    @Test
    public void countAccountsTest() {
        String ledger = null;
        String address = null;
        Object metadata = null;
        // api.countAccounts(ledger, address, metadata);

        // TODO: test validations
    }
    /**
     * Get account by its address
     *
     * 
     */
    @Test
    public void getAccountTest() {
        String ledger = null;
        String address = null;
        // AccountResponse response = api.getAccount(ledger, address);

        // TODO: test validations
    }
    /**
     * List accounts from a ledger
     *
     * List accounts from a ledger, sorted by address in descending order.
     */
    @Test
    public void listAccountsTest() {
        String ledger = null;
        Long pageSize = null;
        Long pageSize2 = null;
        String after = null;
        String address = null;
        Object metadata = null;
        Long balance = null;
        String balanceOperator = null;
        String balanceOperator2 = null;
        String cursor = null;
        String paginationToken = null;
        // AccountsCursorResponse response = api.listAccounts(ledger, pageSize, pageSize2, after, address, metadata, balance, balanceOperator, balanceOperator2, cursor, paginationToken);

        // TODO: test validations
    }
}
