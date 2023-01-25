// TODO: better import syntax?
import {BaseAPIRequestFactory, RequiredError, COLLECTION_FORMATS} from './baseapi';
import {Configuration} from '../configuration';
import {RequestContext, HttpMethod, ResponseContext, HttpFile} from '../http/http';
import * as FormData from "form-data";
import { URLSearchParams } from 'url';
import {ObjectSerializer} from '../models/ObjectSerializer';
import {ApiException} from './exception';
import {canConsumeForm, isCodeInRange} from '../util';
import {SecurityAuthentication} from '../auth/auth';


import { ErrorResponse } from '../models/ErrorResponse';
import { PostTransaction } from '../models/PostTransaction';
import { TransactionResponse } from '../models/TransactionResponse';
import { Transactions } from '../models/Transactions';
import { TransactionsCursorResponse } from '../models/TransactionsCursorResponse';
import { TransactionsResponse } from '../models/TransactionsResponse';

/**
 * no description
 */
export class TransactionsApiRequestFactory extends BaseAPIRequestFactory {

    /**
     * Set the metadata of a transaction by its ID
     * @param ledger Name of the ledger.
     * @param txid Transaction ID.
     * @param requestBody metadata
     */
    public async addMetadataOnTransaction(ledger: string, txid: number, requestBody?: { [key: string]: any; }, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'ledger' is not null or undefined
        if (ledger === null || ledger === undefined) {
            throw new RequiredError("TransactionsApi", "addMetadataOnTransaction", "ledger");
        }


        // verify required parameter 'txid' is not null or undefined
        if (txid === null || txid === undefined) {
            throw new RequiredError("TransactionsApi", "addMetadataOnTransaction", "txid");
        }



        // Path Params
        const localVarPath = '/api/ledger/{ledger}/transactions/{txid}/metadata'
            .replace('{' + 'ledger' + '}', encodeURIComponent(String(ledger)))
            .replace('{' + 'txid' + '}', encodeURIComponent(String(txid)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.POST);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        // Body Params
        const contentType = ObjectSerializer.getPreferredMediaType([
            "application/json"
        ]);
        requestContext.setHeaderParam("Content-Type", contentType);
        const serializedBody = ObjectSerializer.stringify(
            ObjectSerializer.serialize(requestBody, "{ [key: string]: any; }", ""),
            contentType
        );
        requestContext.setBody(serializedBody);

        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["Authorization"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Count the transactions from a ledger
     * @param ledger Name of the ledger.
     * @param reference Filter transactions by reference field.
     * @param account Filter transactions with postings involving given account, either as source or destination (regular expression placed between ^ and $).
     * @param source Filter transactions with postings involving given account at source (regular expression placed between ^ and $).
     * @param destination Filter transactions with postings involving given account at destination (regular expression placed between ^ and $).
     * @param startTime Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). 
     * @param startTime2 Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). Deprecated, please use &#x60;startTime&#x60; instead. 
     * @param endTime Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). 
     * @param endTime2 Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). Deprecated, please use &#x60;endTime&#x60; instead. 
     * @param metadata Filter transactions by metadata key value pairs. Nested objects can be used as seen in the example below.
     */
    public async countTransactions(ledger: string, reference?: string, account?: string, source?: string, destination?: string, startTime?: Date, startTime2?: Date, endTime?: Date, endTime2?: Date, metadata?: any, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'ledger' is not null or undefined
        if (ledger === null || ledger === undefined) {
            throw new RequiredError("TransactionsApi", "countTransactions", "ledger");
        }











        // Path Params
        const localVarPath = '/api/ledger/{ledger}/transactions'
            .replace('{' + 'ledger' + '}', encodeURIComponent(String(ledger)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.HEAD);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")

        // Query Params
        if (reference !== undefined) {
            requestContext.setQueryParam("reference", ObjectSerializer.serialize(reference, "string", ""));
        }

        // Query Params
        if (account !== undefined) {
            requestContext.setQueryParam("account", ObjectSerializer.serialize(account, "string", ""));
        }

        // Query Params
        if (source !== undefined) {
            requestContext.setQueryParam("source", ObjectSerializer.serialize(source, "string", ""));
        }

        // Query Params
        if (destination !== undefined) {
            requestContext.setQueryParam("destination", ObjectSerializer.serialize(destination, "string", ""));
        }

        // Query Params
        if (startTime !== undefined) {
            requestContext.setQueryParam("startTime", ObjectSerializer.serialize(startTime, "Date", "date-time"));
        }

        // Query Params
        if (startTime2 !== undefined) {
            requestContext.setQueryParam("start_time", ObjectSerializer.serialize(startTime2, "Date", "date-time"));
        }

        // Query Params
        if (endTime !== undefined) {
            requestContext.setQueryParam("endTime", ObjectSerializer.serialize(endTime, "Date", "date-time"));
        }

        // Query Params
        if (endTime2 !== undefined) {
            requestContext.setQueryParam("end_time", ObjectSerializer.serialize(endTime2, "Date", "date-time"));
        }

        // Query Params
        if (metadata !== undefined) {
            requestContext.setQueryParam("metadata", ObjectSerializer.serialize(metadata, "any", ""));
        }


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["Authorization"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Create a new transaction to a ledger
     * @param ledger Name of the ledger.
     * @param postTransaction The request body must contain at least one of the following objects:   - &#x60;postings&#x60;: suitable for simple transactions   - &#x60;script&#x60;: enabling more complex transactions with Numscript 
     * @param preview Set the preview mode. Preview mode doesn&#39;t add the logs to the database or publish a message to the message broker.
     */
    public async createTransaction(ledger: string, postTransaction: PostTransaction, preview?: boolean, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'ledger' is not null or undefined
        if (ledger === null || ledger === undefined) {
            throw new RequiredError("TransactionsApi", "createTransaction", "ledger");
        }


        // verify required parameter 'postTransaction' is not null or undefined
        if (postTransaction === null || postTransaction === undefined) {
            throw new RequiredError("TransactionsApi", "createTransaction", "postTransaction");
        }



        // Path Params
        const localVarPath = '/api/ledger/{ledger}/transactions'
            .replace('{' + 'ledger' + '}', encodeURIComponent(String(ledger)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.POST);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")

        // Query Params
        if (preview !== undefined) {
            requestContext.setQueryParam("preview", ObjectSerializer.serialize(preview, "boolean", ""));
        }


        // Body Params
        const contentType = ObjectSerializer.getPreferredMediaType([
            "application/json"
        ]);
        requestContext.setHeaderParam("Content-Type", contentType);
        const serializedBody = ObjectSerializer.stringify(
            ObjectSerializer.serialize(postTransaction, "PostTransaction", ""),
            contentType
        );
        requestContext.setBody(serializedBody);

        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["Authorization"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Create a new batch of transactions to a ledger
     * @param ledger Name of the ledger.
     * @param transactions 
     */
    public async createTransactions(ledger: string, transactions: Transactions, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'ledger' is not null or undefined
        if (ledger === null || ledger === undefined) {
            throw new RequiredError("TransactionsApi", "createTransactions", "ledger");
        }


        // verify required parameter 'transactions' is not null or undefined
        if (transactions === null || transactions === undefined) {
            throw new RequiredError("TransactionsApi", "createTransactions", "transactions");
        }


        // Path Params
        const localVarPath = '/api/ledger/{ledger}/transactions/batch'
            .replace('{' + 'ledger' + '}', encodeURIComponent(String(ledger)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.POST);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        // Body Params
        const contentType = ObjectSerializer.getPreferredMediaType([
            "application/json"
        ]);
        requestContext.setHeaderParam("Content-Type", contentType);
        const serializedBody = ObjectSerializer.stringify(
            ObjectSerializer.serialize(transactions, "Transactions", ""),
            contentType
        );
        requestContext.setBody(serializedBody);

        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["Authorization"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Get transaction from a ledger by its ID
     * @param ledger Name of the ledger.
     * @param txid Transaction ID.
     */
    public async getTransaction(ledger: string, txid: number, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'ledger' is not null or undefined
        if (ledger === null || ledger === undefined) {
            throw new RequiredError("TransactionsApi", "getTransaction", "ledger");
        }


        // verify required parameter 'txid' is not null or undefined
        if (txid === null || txid === undefined) {
            throw new RequiredError("TransactionsApi", "getTransaction", "txid");
        }


        // Path Params
        const localVarPath = '/api/ledger/{ledger}/transactions/{txid}'
            .replace('{' + 'ledger' + '}', encodeURIComponent(String(ledger)))
            .replace('{' + 'txid' + '}', encodeURIComponent(String(txid)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["Authorization"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * List transactions from a ledger, sorted by txid in descending order.
     * List transactions from a ledger
     * @param ledger Name of the ledger.
     * @param pageSize The maximum number of results to return per page. 
     * @param pageSize2 The maximum number of results to return per page. Deprecated, please use &#x60;pageSize&#x60; instead. 
     * @param after Pagination cursor, will return transactions after given txid (in descending order).
     * @param reference Find transactions by reference field.
     * @param account Filter transactions with postings involving given account, either as source or destination (regular expression placed between ^ and $).
     * @param source Filter transactions with postings involving given account at source (regular expression placed between ^ and $).
     * @param destination Filter transactions with postings involving given account at destination (regular expression placed between ^ and $).
     * @param startTime Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). 
     * @param startTime2 Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). Deprecated, please use &#x60;startTime&#x60; instead. 
     * @param endTime Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). 
     * @param endTime2 Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). Deprecated, please use &#x60;endTime&#x60; instead. 
     * @param cursor Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
     * @param paginationToken Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use &#x60;cursor&#x60; instead. 
     * @param metadata Filter transactions by metadata key value pairs. Nested objects can be used as seen in the example below.
     */
    public async listTransactions(ledger: string, pageSize?: number, pageSize2?: number, after?: string, reference?: string, account?: string, source?: string, destination?: string, startTime?: Date, startTime2?: Date, endTime?: Date, endTime2?: Date, cursor?: string, paginationToken?: string, metadata?: any, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'ledger' is not null or undefined
        if (ledger === null || ledger === undefined) {
            throw new RequiredError("TransactionsApi", "listTransactions", "ledger");
        }
















        // Path Params
        const localVarPath = '/api/ledger/{ledger}/transactions'
            .replace('{' + 'ledger' + '}', encodeURIComponent(String(ledger)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")

        // Query Params
        if (pageSize !== undefined) {
            requestContext.setQueryParam("pageSize", ObjectSerializer.serialize(pageSize, "number", "int64"));
        }

        // Query Params
        if (pageSize2 !== undefined) {
            requestContext.setQueryParam("page_size", ObjectSerializer.serialize(pageSize2, "number", "int64"));
        }

        // Query Params
        if (after !== undefined) {
            requestContext.setQueryParam("after", ObjectSerializer.serialize(after, "string", ""));
        }

        // Query Params
        if (reference !== undefined) {
            requestContext.setQueryParam("reference", ObjectSerializer.serialize(reference, "string", ""));
        }

        // Query Params
        if (account !== undefined) {
            requestContext.setQueryParam("account", ObjectSerializer.serialize(account, "string", ""));
        }

        // Query Params
        if (source !== undefined) {
            requestContext.setQueryParam("source", ObjectSerializer.serialize(source, "string", ""));
        }

        // Query Params
        if (destination !== undefined) {
            requestContext.setQueryParam("destination", ObjectSerializer.serialize(destination, "string", ""));
        }

        // Query Params
        if (startTime !== undefined) {
            requestContext.setQueryParam("startTime", ObjectSerializer.serialize(startTime, "Date", "date-time"));
        }

        // Query Params
        if (startTime2 !== undefined) {
            requestContext.setQueryParam("start_time", ObjectSerializer.serialize(startTime2, "Date", "date-time"));
        }

        // Query Params
        if (endTime !== undefined) {
            requestContext.setQueryParam("endTime", ObjectSerializer.serialize(endTime, "Date", "date-time"));
        }

        // Query Params
        if (endTime2 !== undefined) {
            requestContext.setQueryParam("end_time", ObjectSerializer.serialize(endTime2, "Date", "date-time"));
        }

        // Query Params
        if (cursor !== undefined) {
            requestContext.setQueryParam("cursor", ObjectSerializer.serialize(cursor, "string", ""));
        }

        // Query Params
        if (paginationToken !== undefined) {
            requestContext.setQueryParam("pagination_token", ObjectSerializer.serialize(paginationToken, "string", ""));
        }

        // Query Params
        if (metadata !== undefined) {
            requestContext.setQueryParam("metadata", ObjectSerializer.serialize(metadata, "any", ""));
        }


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["Authorization"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Revert a ledger transaction by its ID
     * @param ledger Name of the ledger.
     * @param txid Transaction ID.
     */
    public async revertTransaction(ledger: string, txid: number, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'ledger' is not null or undefined
        if (ledger === null || ledger === undefined) {
            throw new RequiredError("TransactionsApi", "revertTransaction", "ledger");
        }


        // verify required parameter 'txid' is not null or undefined
        if (txid === null || txid === undefined) {
            throw new RequiredError("TransactionsApi", "revertTransaction", "txid");
        }


        // Path Params
        const localVarPath = '/api/ledger/{ledger}/transactions/{txid}/revert'
            .replace('{' + 'ledger' + '}', encodeURIComponent(String(ledger)))
            .replace('{' + 'txid' + '}', encodeURIComponent(String(txid)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.POST);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["Authorization"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

}

export class TransactionsApiResponseProcessor {

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to addMetadataOnTransaction
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async addMetadataOnTransaction(response: ResponseContext): Promise<void > {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("204", response.httpStatusCode)) {
            return;
        }
        if (isCodeInRange("0", response.httpStatusCode)) {
            const body: ErrorResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "ErrorResponse", ""
            ) as ErrorResponse;
            throw new ApiException<ErrorResponse>(response.httpStatusCode, "Error", body, response.headers);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: void = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "void", ""
            ) as void;
            return body;
        }

        throw new ApiException<string | Buffer | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to countTransactions
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async countTransactions(response: ResponseContext): Promise<void > {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            return;
        }
        if (isCodeInRange("0", response.httpStatusCode)) {
            const body: ErrorResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "ErrorResponse", ""
            ) as ErrorResponse;
            throw new ApiException<ErrorResponse>(response.httpStatusCode, "Error", body, response.headers);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: void = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "void", ""
            ) as void;
            return body;
        }

        throw new ApiException<string | Buffer | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to createTransaction
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async createTransaction(response: ResponseContext): Promise<TransactionsResponse > {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: TransactionsResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "TransactionsResponse", ""
            ) as TransactionsResponse;
            return body;
        }
        if (isCodeInRange("0", response.httpStatusCode)) {
            const body: ErrorResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "ErrorResponse", ""
            ) as ErrorResponse;
            throw new ApiException<ErrorResponse>(response.httpStatusCode, "Error", body, response.headers);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: TransactionsResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "TransactionsResponse", ""
            ) as TransactionsResponse;
            return body;
        }

        throw new ApiException<string | Buffer | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to createTransactions
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async createTransactions(response: ResponseContext): Promise<TransactionsResponse > {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: TransactionsResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "TransactionsResponse", ""
            ) as TransactionsResponse;
            return body;
        }
        if (isCodeInRange("0", response.httpStatusCode)) {
            const body: ErrorResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "ErrorResponse", ""
            ) as ErrorResponse;
            throw new ApiException<ErrorResponse>(response.httpStatusCode, "Error", body, response.headers);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: TransactionsResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "TransactionsResponse", ""
            ) as TransactionsResponse;
            return body;
        }

        throw new ApiException<string | Buffer | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getTransaction
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getTransaction(response: ResponseContext): Promise<TransactionResponse > {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: TransactionResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "TransactionResponse", ""
            ) as TransactionResponse;
            return body;
        }
        if (isCodeInRange("0", response.httpStatusCode)) {
            const body: ErrorResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "ErrorResponse", ""
            ) as ErrorResponse;
            throw new ApiException<ErrorResponse>(response.httpStatusCode, "Error", body, response.headers);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: TransactionResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "TransactionResponse", ""
            ) as TransactionResponse;
            return body;
        }

        throw new ApiException<string | Buffer | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to listTransactions
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async listTransactions(response: ResponseContext): Promise<TransactionsCursorResponse > {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: TransactionsCursorResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "TransactionsCursorResponse", ""
            ) as TransactionsCursorResponse;
            return body;
        }
        if (isCodeInRange("0", response.httpStatusCode)) {
            const body: ErrorResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "ErrorResponse", ""
            ) as ErrorResponse;
            throw new ApiException<ErrorResponse>(response.httpStatusCode, "Error", body, response.headers);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: TransactionsCursorResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "TransactionsCursorResponse", ""
            ) as TransactionsCursorResponse;
            return body;
        }

        throw new ApiException<string | Buffer | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to revertTransaction
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async revertTransaction(response: ResponseContext): Promise<TransactionResponse > {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: TransactionResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "TransactionResponse", ""
            ) as TransactionResponse;
            return body;
        }
        if (isCodeInRange("0", response.httpStatusCode)) {
            const body: ErrorResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "ErrorResponse", ""
            ) as ErrorResponse;
            throw new ApiException<ErrorResponse>(response.httpStatusCode, "Error", body, response.headers);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: TransactionResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "TransactionResponse", ""
            ) as TransactionResponse;
            return body;
        }

        throw new ApiException<string | Buffer | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

}
