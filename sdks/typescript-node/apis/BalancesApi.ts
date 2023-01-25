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


import { AggregateBalancesResponse } from '../models/AggregateBalancesResponse';
import { BalancesCursorResponse } from '../models/BalancesCursorResponse';
import { ErrorResponse } from '../models/ErrorResponse';

/**
 * no description
 */
export class BalancesApiRequestFactory extends BaseAPIRequestFactory {

    /**
     * Get the balances from a ledger's account
     * @param ledger Name of the ledger.
     * @param address Filter balances involving given account, either as source or destination.
     * @param after Pagination cursor, will return accounts after given address, in descending order.
     * @param cursor Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
     * @param paginationToken Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. Deprecated, please use &#x60;cursor&#x60; instead.
     */
    public async getBalances(ledger: string, address?: string, after?: string, cursor?: string, paginationToken?: string, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'ledger' is not null or undefined
        if (ledger === null || ledger === undefined) {
            throw new RequiredError("BalancesApi", "getBalances", "ledger");
        }






        // Path Params
        const localVarPath = '/api/ledger/{ledger}/balances'
            .replace('{' + 'ledger' + '}', encodeURIComponent(String(ledger)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")

        // Query Params
        if (address !== undefined) {
            requestContext.setQueryParam("address", ObjectSerializer.serialize(address, "string", ""));
        }

        // Query Params
        if (after !== undefined) {
            requestContext.setQueryParam("after", ObjectSerializer.serialize(after, "string", ""));
        }

        // Query Params
        if (cursor !== undefined) {
            requestContext.setQueryParam("cursor", ObjectSerializer.serialize(cursor, "string", ""));
        }

        // Query Params
        if (paginationToken !== undefined) {
            requestContext.setQueryParam("pagination_token", ObjectSerializer.serialize(paginationToken, "string", ""));
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
     * Get the aggregated balances from selected accounts
     * @param ledger Name of the ledger.
     * @param address Filter balances involving given account, either as source or destination.
     */
    public async getBalancesAggregated(ledger: string, address?: string, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'ledger' is not null or undefined
        if (ledger === null || ledger === undefined) {
            throw new RequiredError("BalancesApi", "getBalancesAggregated", "ledger");
        }



        // Path Params
        const localVarPath = '/api/ledger/{ledger}/aggregate/balances'
            .replace('{' + 'ledger' + '}', encodeURIComponent(String(ledger)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")

        // Query Params
        if (address !== undefined) {
            requestContext.setQueryParam("address", ObjectSerializer.serialize(address, "string", ""));
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

}

export class BalancesApiResponseProcessor {

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getBalances
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getBalances(response: ResponseContext): Promise<BalancesCursorResponse > {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: BalancesCursorResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "BalancesCursorResponse", ""
            ) as BalancesCursorResponse;
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
            const body: BalancesCursorResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "BalancesCursorResponse", ""
            ) as BalancesCursorResponse;
            return body;
        }

        throw new ApiException<string | Buffer | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getBalancesAggregated
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getBalancesAggregated(response: ResponseContext): Promise<AggregateBalancesResponse > {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: AggregateBalancesResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "AggregateBalancesResponse", ""
            ) as AggregateBalancesResponse;
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
            const body: AggregateBalancesResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "AggregateBalancesResponse", ""
            ) as AggregateBalancesResponse;
            return body;
        }

        throw new ApiException<string | Buffer | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

}
