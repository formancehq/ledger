package com.formance.formance.api;

import com.formance.formance.ApiClient;
import com.formance.formance.model.ListUsersResponse;
import com.formance.formance.model.ReadUserResponse;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for UsersApi
 */
public class UsersApiTest {

    private UsersApi api;

    @Before
    public void setup() {
        api = new ApiClient().createService(UsersApi.class);
    }

    /**
     * List users
     *
     * List users
     */
    @Test
    public void listUsersTest() {
        // ListUsersResponse response = api.listUsers();

        // TODO: test validations
    }
    /**
     * Read user
     *
     * Read user
     */
    @Test
    public void readUserTest() {
        String userId = null;
        // ReadUserResponse response = api.readUser(userId);

        // TODO: test validations
    }
}
