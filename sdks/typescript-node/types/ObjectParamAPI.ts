import { ResponseContext, RequestContext, HttpFile } from '../http/http';
import { Configuration} from '../configuration'

import { Account } from '../models/Account';
import { AccountResponse } from '../models/AccountResponse';
import { AccountWithVolumesAndBalances } from '../models/AccountWithVolumesAndBalances';
import { AccountsCursor } from '../models/AccountsCursor';
import { AccountsCursorCursor } from '../models/AccountsCursorCursor';
import { AccountsCursorCursorAllOf } from '../models/AccountsCursorCursorAllOf';
import { AccountsCursorResponse } from '../models/AccountsCursorResponse';
import { AccountsCursorResponseCursor } from '../models/AccountsCursorResponseCursor';
import { AggregateBalancesResponse } from '../models/AggregateBalancesResponse';
import { AssetHolder } from '../models/AssetHolder';
import { Attempt } from '../models/Attempt';
import { AttemptResponse } from '../models/AttemptResponse';
import { Balance } from '../models/Balance';
import { BalanceWithAssets } from '../models/BalanceWithAssets';
import { BalancesCursorResponse } from '../models/BalancesCursorResponse';
import { BalancesCursorResponseCursor } from '../models/BalancesCursorResponseCursor';
import { BankingCircleConfig } from '../models/BankingCircleConfig';
import { Client } from '../models/Client';
import { ClientAllOf } from '../models/ClientAllOf';
import { ClientOptions } from '../models/ClientOptions';
import { ClientSecret } from '../models/ClientSecret';
import { Config } from '../models/Config';
import { ConfigChangeSecret } from '../models/ConfigChangeSecret';
import { ConfigInfo } from '../models/ConfigInfo';
import { ConfigInfoResponse } from '../models/ConfigInfoResponse';
import { ConfigResponse } from '../models/ConfigResponse';
import { ConfigUser } from '../models/ConfigUser';
import { ConfigsResponse } from '../models/ConfigsResponse';
import { ConfigsResponseCursor } from '../models/ConfigsResponseCursor';
import { ConfigsResponseCursorAllOf } from '../models/ConfigsResponseCursorAllOf';
import { ConfirmHoldRequest } from '../models/ConfirmHoldRequest';
import { Connector } from '../models/Connector';
import { ConnectorConfig } from '../models/ConnectorConfig';
import { ConnectorConfigResponse } from '../models/ConnectorConfigResponse';
import { ConnectorsConfigsResponse } from '../models/ConnectorsConfigsResponse';
import { ConnectorsConfigsResponseData } from '../models/ConnectorsConfigsResponseData';
import { ConnectorsConfigsResponseDataConnector } from '../models/ConnectorsConfigsResponseDataConnector';
import { ConnectorsConfigsResponseDataConnectorKey } from '../models/ConnectorsConfigsResponseDataConnectorKey';
import { ConnectorsResponse } from '../models/ConnectorsResponse';
import { ConnectorsResponseDataInner } from '../models/ConnectorsResponseDataInner';
import { Contract } from '../models/Contract';
import { CreateBalanceResponse } from '../models/CreateBalanceResponse';
import { CreateClientResponse } from '../models/CreateClientResponse';
import { CreateScopeResponse } from '../models/CreateScopeResponse';
import { CreateSecretResponse } from '../models/CreateSecretResponse';
import { CreateWalletRequest } from '../models/CreateWalletRequest';
import { CreateWalletResponse } from '../models/CreateWalletResponse';
import { CreateWorkflowResponse } from '../models/CreateWorkflowResponse';
import { CreditWalletRequest } from '../models/CreditWalletRequest';
import { CurrencyCloudConfig } from '../models/CurrencyCloudConfig';
import { Cursor } from '../models/Cursor';
import { CursorBase } from '../models/CursorBase';
import { DebitWalletRequest } from '../models/DebitWalletRequest';
import { DebitWalletResponse } from '../models/DebitWalletResponse';
import { DummyPayConfig } from '../models/DummyPayConfig';
import { ErrorResponse } from '../models/ErrorResponse';
import { ErrorsEnum } from '../models/ErrorsEnum';
import { ExpandedDebitHold } from '../models/ExpandedDebitHold';
import { ExpandedDebitHoldAllOf } from '../models/ExpandedDebitHoldAllOf';
import { GetBalanceResponse } from '../models/GetBalanceResponse';
import { GetHoldResponse } from '../models/GetHoldResponse';
import { GetHoldsResponse } from '../models/GetHoldsResponse';
import { GetHoldsResponseCursor } from '../models/GetHoldsResponseCursor';
import { GetHoldsResponseCursorAllOf } from '../models/GetHoldsResponseCursorAllOf';
import { GetTransactionsResponse } from '../models/GetTransactionsResponse';
import { GetTransactionsResponseCursor } from '../models/GetTransactionsResponseCursor';
import { GetTransactionsResponseCursorAllOf } from '../models/GetTransactionsResponseCursorAllOf';
import { GetWalletResponse } from '../models/GetWalletResponse';
import { GetWorkflowOccurrenceResponse } from '../models/GetWorkflowOccurrenceResponse';
import { GetWorkflowResponse } from '../models/GetWorkflowResponse';
import { Hold } from '../models/Hold';
import { LedgerAccountSubject } from '../models/LedgerAccountSubject';
import { LedgerInfo } from '../models/LedgerInfo';
import { LedgerInfoResponse } from '../models/LedgerInfoResponse';
import { LedgerInfoStorage } from '../models/LedgerInfoStorage';
import { LedgerStorage } from '../models/LedgerStorage';
import { ListBalancesResponse } from '../models/ListBalancesResponse';
import { ListBalancesResponseCursor } from '../models/ListBalancesResponseCursor';
import { ListBalancesResponseCursorAllOf } from '../models/ListBalancesResponseCursorAllOf';
import { ListClientsResponse } from '../models/ListClientsResponse';
import { ListRunsResponse } from '../models/ListRunsResponse';
import { ListRunsResponseCursor } from '../models/ListRunsResponseCursor';
import { ListRunsResponseCursorAllOf } from '../models/ListRunsResponseCursorAllOf';
import { ListScopesResponse } from '../models/ListScopesResponse';
import { ListUsersResponse } from '../models/ListUsersResponse';
import { ListWalletsResponse } from '../models/ListWalletsResponse';
import { ListWalletsResponseCursor } from '../models/ListWalletsResponseCursor';
import { ListWalletsResponseCursorAllOf } from '../models/ListWalletsResponseCursorAllOf';
import { ListWorkflowsResponse } from '../models/ListWorkflowsResponse';
import { Log } from '../models/Log';
import { LogsCursorResponse } from '../models/LogsCursorResponse';
import { LogsCursorResponseCursor } from '../models/LogsCursorResponseCursor';
import { Mapping } from '../models/Mapping';
import { MappingResponse } from '../models/MappingResponse';
import { MigrationInfo } from '../models/MigrationInfo';
import { ModelError } from '../models/ModelError';
import { ModulrConfig } from '../models/ModulrConfig';
import { Monetary } from '../models/Monetary';
import { Payment } from '../models/Payment';
import { PaymentAdjustment } from '../models/PaymentAdjustment';
import { PaymentMetadata } from '../models/PaymentMetadata';
import { PaymentMetadataChangelog } from '../models/PaymentMetadataChangelog';
import { PaymentResponse } from '../models/PaymentResponse';
import { PaymentStatus } from '../models/PaymentStatus';
import { PaymentsAccount } from '../models/PaymentsAccount';
import { PaymentsCursor } from '../models/PaymentsCursor';
import { PaymentsCursorCursor } from '../models/PaymentsCursorCursor';
import { PaymentsCursorCursorAllOf } from '../models/PaymentsCursorCursorAllOf';
import { PostTransaction } from '../models/PostTransaction';
import { PostTransactionScript } from '../models/PostTransactionScript';
import { Posting } from '../models/Posting';
import { Query } from '../models/Query';
import { ReadClientResponse } from '../models/ReadClientResponse';
import { ReadUserResponse } from '../models/ReadUserResponse';
import { ReadWorkflowResponse } from '../models/ReadWorkflowResponse';
import { Response } from '../models/Response';
import { RunWorkflowResponse } from '../models/RunWorkflowResponse';
import { Scope } from '../models/Scope';
import { ScopeAllOf } from '../models/ScopeAllOf';
import { ScopeOptions } from '../models/ScopeOptions';
import { Script } from '../models/Script';
import { ScriptResponse } from '../models/ScriptResponse';
import { Secret } from '../models/Secret';
import { SecretAllOf } from '../models/SecretAllOf';
import { SecretOptions } from '../models/SecretOptions';
import { ServerInfo } from '../models/ServerInfo';
import { StageStatus } from '../models/StageStatus';
import { Stats } from '../models/Stats';
import { StatsResponse } from '../models/StatsResponse';
import { StripeConfig } from '../models/StripeConfig';
import { StripeTransferRequest } from '../models/StripeTransferRequest';
import { Subject } from '../models/Subject';
import { TaskBankingCircle } from '../models/TaskBankingCircle';
import { TaskBankingCircleAllOf } from '../models/TaskBankingCircleAllOf';
import { TaskBankingCircleAllOfDescriptor } from '../models/TaskBankingCircleAllOfDescriptor';
import { TaskBase } from '../models/TaskBase';
import { TaskCurrencyCloud } from '../models/TaskCurrencyCloud';
import { TaskCurrencyCloudAllOf } from '../models/TaskCurrencyCloudAllOf';
import { TaskCurrencyCloudAllOfDescriptor } from '../models/TaskCurrencyCloudAllOfDescriptor';
import { TaskDummyPay } from '../models/TaskDummyPay';
import { TaskDummyPayAllOf } from '../models/TaskDummyPayAllOf';
import { TaskDummyPayAllOfDescriptor } from '../models/TaskDummyPayAllOfDescriptor';
import { TaskModulr } from '../models/TaskModulr';
import { TaskModulrAllOf } from '../models/TaskModulrAllOf';
import { TaskModulrAllOfDescriptor } from '../models/TaskModulrAllOfDescriptor';
import { TaskResponse } from '../models/TaskResponse';
import { TaskStripe } from '../models/TaskStripe';
import { TaskStripeAllOf } from '../models/TaskStripeAllOf';
import { TaskStripeAllOfDescriptor } from '../models/TaskStripeAllOfDescriptor';
import { TaskWise } from '../models/TaskWise';
import { TaskWiseAllOf } from '../models/TaskWiseAllOf';
import { TaskWiseAllOfDescriptor } from '../models/TaskWiseAllOfDescriptor';
import { TasksCursor } from '../models/TasksCursor';
import { TasksCursorCursor } from '../models/TasksCursorCursor';
import { TasksCursorCursorAllOf } from '../models/TasksCursorCursorAllOf';
import { TasksCursorCursorAllOfDataInner } from '../models/TasksCursorCursorAllOfDataInner';
import { Total } from '../models/Total';
import { Transaction } from '../models/Transaction';
import { TransactionData } from '../models/TransactionData';
import { TransactionResponse } from '../models/TransactionResponse';
import { Transactions } from '../models/Transactions';
import { TransactionsCursorResponse } from '../models/TransactionsCursorResponse';
import { TransactionsCursorResponseCursor } from '../models/TransactionsCursorResponseCursor';
import { TransactionsResponse } from '../models/TransactionsResponse';
import { UpdateWalletRequest } from '../models/UpdateWalletRequest';
import { User } from '../models/User';
import { Volume } from '../models/Volume';
import { Wallet } from '../models/Wallet';
import { WalletSubject } from '../models/WalletSubject';
import { WalletWithBalances } from '../models/WalletWithBalances';
import { WalletWithBalancesBalances } from '../models/WalletWithBalancesBalances';
import { WalletsErrorResponse } from '../models/WalletsErrorResponse';
import { WalletsTransaction } from '../models/WalletsTransaction';
import { WalletsVolume } from '../models/WalletsVolume';
import { WebhooksConfig } from '../models/WebhooksConfig';
import { WiseConfig } from '../models/WiseConfig';
import { Workflow } from '../models/Workflow';
import { WorkflowConfig } from '../models/WorkflowConfig';
import { WorkflowOccurrence } from '../models/WorkflowOccurrence';

import { ObservableAccountsApi } from "./ObservableAPI";
import { AccountsApiRequestFactory, AccountsApiResponseProcessor} from "../apis/AccountsApi";

export interface AccountsApiAddMetadataToAccountRequest {
    /**
     * Name of the ledger.
     * @type string
     * @memberof AccountsApiaddMetadataToAccount
     */
    ledger: string
    /**
     * Exact address of the account. It must match the following regular expressions pattern: &#x60;&#x60;&#x60; ^\\w+(:\\w+)*$ &#x60;&#x60;&#x60; 
     * @type string
     * @memberof AccountsApiaddMetadataToAccount
     */
    address: string
    /**
     * metadata
     * @type { [key: string]: any; }
     * @memberof AccountsApiaddMetadataToAccount
     */
    requestBody: { [key: string]: any; }
}

export interface AccountsApiCountAccountsRequest {
    /**
     * Name of the ledger.
     * @type string
     * @memberof AccountsApicountAccounts
     */
    ledger: string
    /**
     * Filter accounts by address pattern (regular expression placed between ^ and $).
     * @type string
     * @memberof AccountsApicountAccounts
     */
    address?: string
    /**
     * Filter accounts by metadata key value pairs. Nested objects can be used as seen in the example below.
     * @type any
     * @memberof AccountsApicountAccounts
     */
    metadata?: any
}

export interface AccountsApiGetAccountRequest {
    /**
     * Name of the ledger.
     * @type string
     * @memberof AccountsApigetAccount
     */
    ledger: string
    /**
     * Exact address of the account. It must match the following regular expressions pattern: &#x60;&#x60;&#x60; ^\\w+(:\\w+)*$ &#x60;&#x60;&#x60; 
     * @type string
     * @memberof AccountsApigetAccount
     */
    address: string
}

export interface AccountsApiListAccountsRequest {
    /**
     * Name of the ledger.
     * @type string
     * @memberof AccountsApilistAccounts
     */
    ledger: string
    /**
     * The maximum number of results to return per page. 
     * @type number
     * @memberof AccountsApilistAccounts
     */
    pageSize?: number
    /**
     * The maximum number of results to return per page. Deprecated, please use &#x60;pageSize&#x60; instead. 
     * @type number
     * @memberof AccountsApilistAccounts
     */
    pageSize2?: number
    /**
     * Pagination cursor, will return accounts after given address, in descending order.
     * @type string
     * @memberof AccountsApilistAccounts
     */
    after?: string
    /**
     * Filter accounts by address pattern (regular expression placed between ^ and $).
     * @type string
     * @memberof AccountsApilistAccounts
     */
    address?: string
    /**
     * Filter accounts by metadata key value pairs. Nested objects can be used as seen in the example below.
     * @type any
     * @memberof AccountsApilistAccounts
     */
    metadata?: any
    /**
     * Filter accounts by their balance (default operator is gte)
     * @type number
     * @memberof AccountsApilistAccounts
     */
    balance?: number
    /**
     * Operator used for the filtering of balances can be greater than/equal, less than/equal, greater than, less than, equal or not. 
     * @type &#39;gte&#39; | &#39;lte&#39; | &#39;gt&#39; | &#39;lt&#39; | &#39;e&#39; | &#39;ne&#39;
     * @memberof AccountsApilistAccounts
     */
    balanceOperator?: 'gte' | 'lte' | 'gt' | 'lt' | 'e' | 'ne'
    /**
     * Operator used for the filtering of balances can be greater than/equal, less than/equal, greater than, less than, equal or not. Deprecated, please use &#x60;balanceOperator&#x60; instead. 
     * @type &#39;gte&#39; | &#39;lte&#39; | &#39;gt&#39; | &#39;lt&#39; | &#39;e&#39; | &#39;ne&#39;
     * @memberof AccountsApilistAccounts
     */
    balanceOperator2?: 'gte' | 'lte' | 'gt' | 'lt' | 'e' | 'ne'
    /**
     * Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
     * @type string
     * @memberof AccountsApilistAccounts
     */
    cursor?: string
    /**
     * Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use &#x60;cursor&#x60; instead. 
     * @type string
     * @memberof AccountsApilistAccounts
     */
    paginationToken?: string
}

export class ObjectAccountsApi {
    private api: ObservableAccountsApi

    public constructor(configuration: Configuration, requestFactory?: AccountsApiRequestFactory, responseProcessor?: AccountsApiResponseProcessor) {
        this.api = new ObservableAccountsApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Add metadata to an account
     * @param param the request object
     */
    public addMetadataToAccount(param: AccountsApiAddMetadataToAccountRequest, options?: Configuration): Promise<void> {
        return this.api.addMetadataToAccount(param.ledger, param.address, param.requestBody,  options).toPromise();
    }

    /**
     * Count the accounts from a ledger
     * @param param the request object
     */
    public countAccounts(param: AccountsApiCountAccountsRequest, options?: Configuration): Promise<void> {
        return this.api.countAccounts(param.ledger, param.address, param.metadata,  options).toPromise();
    }

    /**
     * Get account by its address
     * @param param the request object
     */
    public getAccount(param: AccountsApiGetAccountRequest, options?: Configuration): Promise<AccountResponse> {
        return this.api.getAccount(param.ledger, param.address,  options).toPromise();
    }

    /**
     * List accounts from a ledger, sorted by address in descending order.
     * List accounts from a ledger
     * @param param the request object
     */
    public listAccounts(param: AccountsApiListAccountsRequest, options?: Configuration): Promise<AccountsCursorResponse> {
        return this.api.listAccounts(param.ledger, param.pageSize, param.pageSize2, param.after, param.address, param.metadata, param.balance, param.balanceOperator, param.balanceOperator2, param.cursor, param.paginationToken,  options).toPromise();
    }

}

import { ObservableBalancesApi } from "./ObservableAPI";
import { BalancesApiRequestFactory, BalancesApiResponseProcessor} from "../apis/BalancesApi";

export interface BalancesApiGetBalancesRequest {
    /**
     * Name of the ledger.
     * @type string
     * @memberof BalancesApigetBalances
     */
    ledger: string
    /**
     * Filter balances involving given account, either as source or destination.
     * @type string
     * @memberof BalancesApigetBalances
     */
    address?: string
    /**
     * Pagination cursor, will return accounts after given address, in descending order.
     * @type string
     * @memberof BalancesApigetBalances
     */
    after?: string
    /**
     * Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
     * @type string
     * @memberof BalancesApigetBalances
     */
    cursor?: string
    /**
     * Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. Deprecated, please use &#x60;cursor&#x60; instead.
     * @type string
     * @memberof BalancesApigetBalances
     */
    paginationToken?: string
}

export interface BalancesApiGetBalancesAggregatedRequest {
    /**
     * Name of the ledger.
     * @type string
     * @memberof BalancesApigetBalancesAggregated
     */
    ledger: string
    /**
     * Filter balances involving given account, either as source or destination.
     * @type string
     * @memberof BalancesApigetBalancesAggregated
     */
    address?: string
}

export class ObjectBalancesApi {
    private api: ObservableBalancesApi

    public constructor(configuration: Configuration, requestFactory?: BalancesApiRequestFactory, responseProcessor?: BalancesApiResponseProcessor) {
        this.api = new ObservableBalancesApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Get the balances from a ledger's account
     * @param param the request object
     */
    public getBalances(param: BalancesApiGetBalancesRequest, options?: Configuration): Promise<BalancesCursorResponse> {
        return this.api.getBalances(param.ledger, param.address, param.after, param.cursor, param.paginationToken,  options).toPromise();
    }

    /**
     * Get the aggregated balances from selected accounts
     * @param param the request object
     */
    public getBalancesAggregated(param: BalancesApiGetBalancesAggregatedRequest, options?: Configuration): Promise<AggregateBalancesResponse> {
        return this.api.getBalancesAggregated(param.ledger, param.address,  options).toPromise();
    }

}

import { ObservableClientsApi } from "./ObservableAPI";
import { ClientsApiRequestFactory, ClientsApiResponseProcessor} from "../apis/ClientsApi";

export interface ClientsApiAddScopeToClientRequest {
    /**
     * Client ID
     * @type string
     * @memberof ClientsApiaddScopeToClient
     */
    clientId: string
    /**
     * Scope ID
     * @type string
     * @memberof ClientsApiaddScopeToClient
     */
    scopeId: string
}

export interface ClientsApiCreateClientRequest {
    /**
     * 
     * @type ClientOptions
     * @memberof ClientsApicreateClient
     */
    body?: ClientOptions
}

export interface ClientsApiCreateSecretRequest {
    /**
     * Client ID
     * @type string
     * @memberof ClientsApicreateSecret
     */
    clientId: string
    /**
     * 
     * @type SecretOptions
     * @memberof ClientsApicreateSecret
     */
    body?: SecretOptions
}

export interface ClientsApiDeleteClientRequest {
    /**
     * Client ID
     * @type string
     * @memberof ClientsApideleteClient
     */
    clientId: string
}

export interface ClientsApiDeleteScopeFromClientRequest {
    /**
     * Client ID
     * @type string
     * @memberof ClientsApideleteScopeFromClient
     */
    clientId: string
    /**
     * Scope ID
     * @type string
     * @memberof ClientsApideleteScopeFromClient
     */
    scopeId: string
}

export interface ClientsApiDeleteSecretRequest {
    /**
     * Client ID
     * @type string
     * @memberof ClientsApideleteSecret
     */
    clientId: string
    /**
     * Secret ID
     * @type string
     * @memberof ClientsApideleteSecret
     */
    secretId: string
}

export interface ClientsApiListClientsRequest {
}

export interface ClientsApiReadClientRequest {
    /**
     * Client ID
     * @type string
     * @memberof ClientsApireadClient
     */
    clientId: string
}

export interface ClientsApiUpdateClientRequest {
    /**
     * Client ID
     * @type string
     * @memberof ClientsApiupdateClient
     */
    clientId: string
    /**
     * 
     * @type ClientOptions
     * @memberof ClientsApiupdateClient
     */
    body?: ClientOptions
}

export class ObjectClientsApi {
    private api: ObservableClientsApi

    public constructor(configuration: Configuration, requestFactory?: ClientsApiRequestFactory, responseProcessor?: ClientsApiResponseProcessor) {
        this.api = new ObservableClientsApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Add scope to client
     * @param param the request object
     */
    public addScopeToClient(param: ClientsApiAddScopeToClientRequest, options?: Configuration): Promise<void> {
        return this.api.addScopeToClient(param.clientId, param.scopeId,  options).toPromise();
    }

    /**
     * Create client
     * @param param the request object
     */
    public createClient(param: ClientsApiCreateClientRequest = {}, options?: Configuration): Promise<CreateClientResponse> {
        return this.api.createClient(param.body,  options).toPromise();
    }

    /**
     * Add a secret to a client
     * @param param the request object
     */
    public createSecret(param: ClientsApiCreateSecretRequest, options?: Configuration): Promise<CreateSecretResponse> {
        return this.api.createSecret(param.clientId, param.body,  options).toPromise();
    }

    /**
     * Delete client
     * @param param the request object
     */
    public deleteClient(param: ClientsApiDeleteClientRequest, options?: Configuration): Promise<void> {
        return this.api.deleteClient(param.clientId,  options).toPromise();
    }

    /**
     * Delete scope from client
     * @param param the request object
     */
    public deleteScopeFromClient(param: ClientsApiDeleteScopeFromClientRequest, options?: Configuration): Promise<void> {
        return this.api.deleteScopeFromClient(param.clientId, param.scopeId,  options).toPromise();
    }

    /**
     * Delete a secret from a client
     * @param param the request object
     */
    public deleteSecret(param: ClientsApiDeleteSecretRequest, options?: Configuration): Promise<void> {
        return this.api.deleteSecret(param.clientId, param.secretId,  options).toPromise();
    }

    /**
     * List clients
     * @param param the request object
     */
    public listClients(param: ClientsApiListClientsRequest = {}, options?: Configuration): Promise<ListClientsResponse> {
        return this.api.listClients( options).toPromise();
    }

    /**
     * Read client
     * @param param the request object
     */
    public readClient(param: ClientsApiReadClientRequest, options?: Configuration): Promise<ReadClientResponse> {
        return this.api.readClient(param.clientId,  options).toPromise();
    }

    /**
     * Update client
     * @param param the request object
     */
    public updateClient(param: ClientsApiUpdateClientRequest, options?: Configuration): Promise<CreateClientResponse> {
        return this.api.updateClient(param.clientId, param.body,  options).toPromise();
    }

}

import { ObservableDefaultApi } from "./ObservableAPI";
import { DefaultApiRequestFactory, DefaultApiResponseProcessor} from "../apis/DefaultApi";

export interface DefaultApiGetServerInfoRequest {
}

export interface DefaultApiPaymentsgetServerInfoRequest {
}

export interface DefaultApiSearchgetServerInfoRequest {
}

export class ObjectDefaultApi {
    private api: ObservableDefaultApi

    public constructor(configuration: Configuration, requestFactory?: DefaultApiRequestFactory, responseProcessor?: DefaultApiResponseProcessor) {
        this.api = new ObservableDefaultApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Get server info
     * @param param the request object
     */
    public getServerInfo(param: DefaultApiGetServerInfoRequest = {}, options?: Configuration): Promise<ServerInfo> {
        return this.api.getServerInfo( options).toPromise();
    }

    /**
     * Get server info
     * @param param the request object
     */
    public paymentsgetServerInfo(param: DefaultApiPaymentsgetServerInfoRequest = {}, options?: Configuration): Promise<ServerInfo> {
        return this.api.paymentsgetServerInfo( options).toPromise();
    }

    /**
     * Get server info
     * @param param the request object
     */
    public searchgetServerInfo(param: DefaultApiSearchgetServerInfoRequest = {}, options?: Configuration): Promise<ServerInfo> {
        return this.api.searchgetServerInfo( options).toPromise();
    }

}

import { ObservableLedgerApi } from "./ObservableAPI";
import { LedgerApiRequestFactory, LedgerApiResponseProcessor} from "../apis/LedgerApi";

export interface LedgerApiGetLedgerInfoRequest {
    /**
     * Name of the ledger.
     * @type string
     * @memberof LedgerApigetLedgerInfo
     */
    ledger: string
}

export class ObjectLedgerApi {
    private api: ObservableLedgerApi

    public constructor(configuration: Configuration, requestFactory?: LedgerApiRequestFactory, responseProcessor?: LedgerApiResponseProcessor) {
        this.api = new ObservableLedgerApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Get information about a ledger
     * @param param the request object
     */
    public getLedgerInfo(param: LedgerApiGetLedgerInfoRequest, options?: Configuration): Promise<LedgerInfoResponse> {
        return this.api.getLedgerInfo(param.ledger,  options).toPromise();
    }

}

import { ObservableLogsApi } from "./ObservableAPI";
import { LogsApiRequestFactory, LogsApiResponseProcessor} from "../apis/LogsApi";

export interface LogsApiListLogsRequest {
    /**
     * Name of the ledger.
     * @type string
     * @memberof LogsApilistLogs
     */
    ledger: string
    /**
     * The maximum number of results to return per page. 
     * @type number
     * @memberof LogsApilistLogs
     */
    pageSize?: number
    /**
     * The maximum number of results to return per page. Deprecated, please use &#x60;pageSize&#x60; instead. 
     * @type number
     * @memberof LogsApilistLogs
     */
    pageSize2?: number
    /**
     * Pagination cursor, will return the logs after a given ID. (in descending order).
     * @type string
     * @memberof LogsApilistLogs
     */
    after?: string
    /**
     * Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). 
     * @type Date
     * @memberof LogsApilistLogs
     */
    startTime?: Date
    /**
     * Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). Deprecated, please use &#x60;startTime&#x60; instead. 
     * @type Date
     * @memberof LogsApilistLogs
     */
    startTime2?: Date
    /**
     * Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). 
     * @type Date
     * @memberof LogsApilistLogs
     */
    endTime?: Date
    /**
     * Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). Deprecated, please use &#x60;endTime&#x60; instead. 
     * @type Date
     * @memberof LogsApilistLogs
     */
    endTime2?: Date
    /**
     * Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
     * @type string
     * @memberof LogsApilistLogs
     */
    cursor?: string
    /**
     * Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use &#x60;cursor&#x60; instead. 
     * @type string
     * @memberof LogsApilistLogs
     */
    paginationToken?: string
}

export class ObjectLogsApi {
    private api: ObservableLogsApi

    public constructor(configuration: Configuration, requestFactory?: LogsApiRequestFactory, responseProcessor?: LogsApiResponseProcessor) {
        this.api = new ObservableLogsApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * List the logs from a ledger, sorted by ID in descending order.
     * List the logs from a ledger
     * @param param the request object
     */
    public listLogs(param: LogsApiListLogsRequest, options?: Configuration): Promise<LogsCursorResponse> {
        return this.api.listLogs(param.ledger, param.pageSize, param.pageSize2, param.after, param.startTime, param.startTime2, param.endTime, param.endTime2, param.cursor, param.paginationToken,  options).toPromise();
    }

}

import { ObservableMappingApi } from "./ObservableAPI";
import { MappingApiRequestFactory, MappingApiResponseProcessor} from "../apis/MappingApi";

export interface MappingApiGetMappingRequest {
    /**
     * Name of the ledger.
     * @type string
     * @memberof MappingApigetMapping
     */
    ledger: string
}

export interface MappingApiUpdateMappingRequest {
    /**
     * Name of the ledger.
     * @type string
     * @memberof MappingApiupdateMapping
     */
    ledger: string
    /**
     * 
     * @type Mapping
     * @memberof MappingApiupdateMapping
     */
    mapping: Mapping
}

export class ObjectMappingApi {
    private api: ObservableMappingApi

    public constructor(configuration: Configuration, requestFactory?: MappingApiRequestFactory, responseProcessor?: MappingApiResponseProcessor) {
        this.api = new ObservableMappingApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Get the mapping of a ledger
     * @param param the request object
     */
    public getMapping(param: MappingApiGetMappingRequest, options?: Configuration): Promise<MappingResponse> {
        return this.api.getMapping(param.ledger,  options).toPromise();
    }

    /**
     * Update the mapping of a ledger
     * @param param the request object
     */
    public updateMapping(param: MappingApiUpdateMappingRequest, options?: Configuration): Promise<MappingResponse> {
        return this.api.updateMapping(param.ledger, param.mapping,  options).toPromise();
    }

}

import { ObservableOrchestrationApi } from "./ObservableAPI";
import { OrchestrationApiRequestFactory, OrchestrationApiResponseProcessor} from "../apis/OrchestrationApi";

export interface OrchestrationApiCreateWorkflowRequest {
    /**
     * 
     * @type WorkflowConfig
     * @memberof OrchestrationApicreateWorkflow
     */
    body?: WorkflowConfig
}

export interface OrchestrationApiGetFlowRequest {
    /**
     * The flow id
     * @type string
     * @memberof OrchestrationApigetFlow
     */
    flowId: string
}

export interface OrchestrationApiGetWorkflowOccurrenceRequest {
    /**
     * The flow id
     * @type string
     * @memberof OrchestrationApigetWorkflowOccurrence
     */
    flowId: string
    /**
     * The occurrence id
     * @type string
     * @memberof OrchestrationApigetWorkflowOccurrence
     */
    runId: string
}

export interface OrchestrationApiListFlowsRequest {
}

export interface OrchestrationApiListRunsRequest {
    /**
     * The flow id
     * @type string
     * @memberof OrchestrationApilistRuns
     */
    flowId: string
}

export interface OrchestrationApiOrchestrationgetServerInfoRequest {
}

export interface OrchestrationApiRunWorkflowRequest {
    /**
     * The flow id
     * @type string
     * @memberof OrchestrationApirunWorkflow
     */
    flowId: string
    /**
     * Wait end of the workflow before return
     * @type boolean
     * @memberof OrchestrationApirunWorkflow
     */
    wait?: boolean
    /**
     * 
     * @type { [key: string]: string; }
     * @memberof OrchestrationApirunWorkflow
     */
    requestBody?: { [key: string]: string; }
}

export class ObjectOrchestrationApi {
    private api: ObservableOrchestrationApi

    public constructor(configuration: Configuration, requestFactory?: OrchestrationApiRequestFactory, responseProcessor?: OrchestrationApiResponseProcessor) {
        this.api = new ObservableOrchestrationApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Create a workflow
     * Create workflow
     * @param param the request object
     */
    public createWorkflow(param: OrchestrationApiCreateWorkflowRequest = {}, options?: Configuration): Promise<CreateWorkflowResponse> {
        return this.api.createWorkflow(param.body,  options).toPromise();
    }

    /**
     * Get a flow by id
     * Get a flow by id
     * @param param the request object
     */
    public getFlow(param: OrchestrationApiGetFlowRequest, options?: Configuration): Promise<GetWorkflowResponse> {
        return this.api.getFlow(param.flowId,  options).toPromise();
    }

    /**
     * Get a workflow occurrence by id
     * Get a workflow occurrence by id
     * @param param the request object
     */
    public getWorkflowOccurrence(param: OrchestrationApiGetWorkflowOccurrenceRequest, options?: Configuration): Promise<GetWorkflowOccurrenceResponse> {
        return this.api.getWorkflowOccurrence(param.flowId, param.runId,  options).toPromise();
    }

    /**
     * List registered flows
     * List registered flows
     * @param param the request object
     */
    public listFlows(param: OrchestrationApiListFlowsRequest = {}, options?: Configuration): Promise<ListWorkflowsResponse> {
        return this.api.listFlows( options).toPromise();
    }

    /**
     * List occurrences of a workflow
     * List occurrences of a workflow
     * @param param the request object
     */
    public listRuns(param: OrchestrationApiListRunsRequest, options?: Configuration): Promise<ListRunsResponse> {
        return this.api.listRuns(param.flowId,  options).toPromise();
    }

    /**
     * Get server info
     * @param param the request object
     */
    public orchestrationgetServerInfo(param: OrchestrationApiOrchestrationgetServerInfoRequest = {}, options?: Configuration): Promise<ServerInfo> {
        return this.api.orchestrationgetServerInfo( options).toPromise();
    }

    /**
     * Run workflow
     * Run workflow
     * @param param the request object
     */
    public runWorkflow(param: OrchestrationApiRunWorkflowRequest, options?: Configuration): Promise<RunWorkflowResponse> {
        return this.api.runWorkflow(param.flowId, param.wait, param.requestBody,  options).toPromise();
    }

}

import { ObservablePaymentsApi } from "./ObservableAPI";
import { PaymentsApiRequestFactory, PaymentsApiResponseProcessor} from "../apis/PaymentsApi";

export interface PaymentsApiConnectorsStripeTransferRequest {
    /**
     * 
     * @type StripeTransferRequest
     * @memberof PaymentsApiconnectorsStripeTransfer
     */
    stripeTransferRequest: StripeTransferRequest
}

export interface PaymentsApiGetConnectorTaskRequest {
    /**
     * The name of the connector.
     * @type Connector
     * @memberof PaymentsApigetConnectorTask
     */
    connector: Connector
    /**
     * The task ID.
     * @type string
     * @memberof PaymentsApigetConnectorTask
     */
    taskId: string
}

export interface PaymentsApiGetPaymentRequest {
    /**
     * The payment ID.
     * @type string
     * @memberof PaymentsApigetPayment
     */
    paymentId: string
}

export interface PaymentsApiInstallConnectorRequest {
    /**
     * The name of the connector.
     * @type Connector
     * @memberof PaymentsApiinstallConnector
     */
    connector: Connector
    /**
     * 
     * @type ConnectorConfig
     * @memberof PaymentsApiinstallConnector
     */
    connectorConfig: ConnectorConfig
}

export interface PaymentsApiListAllConnectorsRequest {
}

export interface PaymentsApiListConfigsAvailableConnectorsRequest {
}

export interface PaymentsApiListConnectorTasksRequest {
    /**
     * The name of the connector.
     * @type Connector
     * @memberof PaymentsApilistConnectorTasks
     */
    connector: Connector
    /**
     * The maximum number of results to return per page. 
     * @type number
     * @memberof PaymentsApilistConnectorTasks
     */
    pageSize?: number
    /**
     * Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
     * @type string
     * @memberof PaymentsApilistConnectorTasks
     */
    cursor?: string
}

export interface PaymentsApiListPaymentsRequest {
    /**
     * The maximum number of results to return per page. 
     * @type number
     * @memberof PaymentsApilistPayments
     */
    pageSize?: number
    /**
     * Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
     * @type string
     * @memberof PaymentsApilistPayments
     */
    cursor?: string
    /**
     * Fields used to sort payments (default is date:desc).
     * @type Array&lt;string&gt;
     * @memberof PaymentsApilistPayments
     */
    sort?: Array<string>
}

export interface PaymentsApiPaymentslistAccountsRequest {
    /**
     * The maximum number of results to return per page. 
     * @type number
     * @memberof PaymentsApipaymentslistAccounts
     */
    pageSize?: number
    /**
     * Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
     * @type string
     * @memberof PaymentsApipaymentslistAccounts
     */
    cursor?: string
    /**
     * Fields used to sort payments (default is date:desc).
     * @type Array&lt;string&gt;
     * @memberof PaymentsApipaymentslistAccounts
     */
    sort?: Array<string>
}

export interface PaymentsApiReadConnectorConfigRequest {
    /**
     * The name of the connector.
     * @type Connector
     * @memberof PaymentsApireadConnectorConfig
     */
    connector: Connector
}

export interface PaymentsApiResetConnectorRequest {
    /**
     * The name of the connector.
     * @type Connector
     * @memberof PaymentsApiresetConnector
     */
    connector: Connector
}

export interface PaymentsApiUninstallConnectorRequest {
    /**
     * The name of the connector.
     * @type Connector
     * @memberof PaymentsApiuninstallConnector
     */
    connector: Connector
}

export class ObjectPaymentsApi {
    private api: ObservablePaymentsApi

    public constructor(configuration: Configuration, requestFactory?: PaymentsApiRequestFactory, responseProcessor?: PaymentsApiResponseProcessor) {
        this.api = new ObservablePaymentsApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Execute a transfer between two Stripe accounts.
     * Transfer funds between Stripe accounts
     * @param param the request object
     */
    public connectorsStripeTransfer(param: PaymentsApiConnectorsStripeTransferRequest, options?: Configuration): Promise<any> {
        return this.api.connectorsStripeTransfer(param.stripeTransferRequest,  options).toPromise();
    }

    /**
     * Get a specific task associated to the connector.
     * Read a specific task of the connector
     * @param param the request object
     */
    public getConnectorTask(param: PaymentsApiGetConnectorTaskRequest, options?: Configuration): Promise<TaskResponse> {
        return this.api.getConnectorTask(param.connector, param.taskId,  options).toPromise();
    }

    /**
     * Get a payment
     * @param param the request object
     */
    public getPayment(param: PaymentsApiGetPaymentRequest, options?: Configuration): Promise<PaymentResponse> {
        return this.api.getPayment(param.paymentId,  options).toPromise();
    }

    /**
     * Install a connector by its name and config.
     * Install a connector
     * @param param the request object
     */
    public installConnector(param: PaymentsApiInstallConnectorRequest, options?: Configuration): Promise<void> {
        return this.api.installConnector(param.connector, param.connectorConfig,  options).toPromise();
    }

    /**
     * List all installed connectors.
     * List all installed connectors
     * @param param the request object
     */
    public listAllConnectors(param: PaymentsApiListAllConnectorsRequest = {}, options?: Configuration): Promise<ConnectorsResponse> {
        return this.api.listAllConnectors( options).toPromise();
    }

    /**
     * List the configs of each available connector.
     * List the configs of each available connector
     * @param param the request object
     */
    public listConfigsAvailableConnectors(param: PaymentsApiListConfigsAvailableConnectorsRequest = {}, options?: Configuration): Promise<ConnectorsConfigsResponse> {
        return this.api.listConfigsAvailableConnectors( options).toPromise();
    }

    /**
     * List all tasks associated with this connector.
     * List tasks from a connector
     * @param param the request object
     */
    public listConnectorTasks(param: PaymentsApiListConnectorTasksRequest, options?: Configuration): Promise<TasksCursor> {
        return this.api.listConnectorTasks(param.connector, param.pageSize, param.cursor,  options).toPromise();
    }

    /**
     * List payments
     * @param param the request object
     */
    public listPayments(param: PaymentsApiListPaymentsRequest = {}, options?: Configuration): Promise<PaymentsCursor> {
        return this.api.listPayments(param.pageSize, param.cursor, param.sort,  options).toPromise();
    }

    /**
     * List accounts
     * @param param the request object
     */
    public paymentslistAccounts(param: PaymentsApiPaymentslistAccountsRequest = {}, options?: Configuration): Promise<AccountsCursor> {
        return this.api.paymentslistAccounts(param.pageSize, param.cursor, param.sort,  options).toPromise();
    }

    /**
     * Read connector config
     * Read the config of a connector
     * @param param the request object
     */
    public readConnectorConfig(param: PaymentsApiReadConnectorConfigRequest, options?: Configuration): Promise<ConnectorConfigResponse> {
        return this.api.readConnectorConfig(param.connector,  options).toPromise();
    }

    /**
     * Reset a connector by its name. It will remove the connector and ALL PAYMENTS generated with it. 
     * Reset a connector
     * @param param the request object
     */
    public resetConnector(param: PaymentsApiResetConnectorRequest, options?: Configuration): Promise<void> {
        return this.api.resetConnector(param.connector,  options).toPromise();
    }

    /**
     * Uninstall a connector by its name.
     * Uninstall a connector
     * @param param the request object
     */
    public uninstallConnector(param: PaymentsApiUninstallConnectorRequest, options?: Configuration): Promise<void> {
        return this.api.uninstallConnector(param.connector,  options).toPromise();
    }

}

import { ObservableScopesApi } from "./ObservableAPI";
import { ScopesApiRequestFactory, ScopesApiResponseProcessor} from "../apis/ScopesApi";

export interface ScopesApiAddTransientScopeRequest {
    /**
     * Scope ID
     * @type string
     * @memberof ScopesApiaddTransientScope
     */
    scopeId: string
    /**
     * Transient scope ID
     * @type string
     * @memberof ScopesApiaddTransientScope
     */
    transientScopeId: string
}

export interface ScopesApiCreateScopeRequest {
    /**
     * 
     * @type ScopeOptions
     * @memberof ScopesApicreateScope
     */
    body?: ScopeOptions
}

export interface ScopesApiDeleteScopeRequest {
    /**
     * Scope ID
     * @type string
     * @memberof ScopesApideleteScope
     */
    scopeId: string
}

export interface ScopesApiDeleteTransientScopeRequest {
    /**
     * Scope ID
     * @type string
     * @memberof ScopesApideleteTransientScope
     */
    scopeId: string
    /**
     * Transient scope ID
     * @type string
     * @memberof ScopesApideleteTransientScope
     */
    transientScopeId: string
}

export interface ScopesApiListScopesRequest {
}

export interface ScopesApiReadScopeRequest {
    /**
     * Scope ID
     * @type string
     * @memberof ScopesApireadScope
     */
    scopeId: string
}

export interface ScopesApiUpdateScopeRequest {
    /**
     * Scope ID
     * @type string
     * @memberof ScopesApiupdateScope
     */
    scopeId: string
    /**
     * 
     * @type ScopeOptions
     * @memberof ScopesApiupdateScope
     */
    body?: ScopeOptions
}

export class ObjectScopesApi {
    private api: ObservableScopesApi

    public constructor(configuration: Configuration, requestFactory?: ScopesApiRequestFactory, responseProcessor?: ScopesApiResponseProcessor) {
        this.api = new ObservableScopesApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Add a transient scope to a scope
     * Add a transient scope to a scope
     * @param param the request object
     */
    public addTransientScope(param: ScopesApiAddTransientScopeRequest, options?: Configuration): Promise<void> {
        return this.api.addTransientScope(param.scopeId, param.transientScopeId,  options).toPromise();
    }

    /**
     * Create scope
     * Create scope
     * @param param the request object
     */
    public createScope(param: ScopesApiCreateScopeRequest = {}, options?: Configuration): Promise<CreateScopeResponse> {
        return this.api.createScope(param.body,  options).toPromise();
    }

    /**
     * Delete scope
     * Delete scope
     * @param param the request object
     */
    public deleteScope(param: ScopesApiDeleteScopeRequest, options?: Configuration): Promise<void> {
        return this.api.deleteScope(param.scopeId,  options).toPromise();
    }

    /**
     * Delete a transient scope from a scope
     * Delete a transient scope from a scope
     * @param param the request object
     */
    public deleteTransientScope(param: ScopesApiDeleteTransientScopeRequest, options?: Configuration): Promise<void> {
        return this.api.deleteTransientScope(param.scopeId, param.transientScopeId,  options).toPromise();
    }

    /**
     * List Scopes
     * List scopes
     * @param param the request object
     */
    public listScopes(param: ScopesApiListScopesRequest = {}, options?: Configuration): Promise<ListScopesResponse> {
        return this.api.listScopes( options).toPromise();
    }

    /**
     * Read scope
     * Read scope
     * @param param the request object
     */
    public readScope(param: ScopesApiReadScopeRequest, options?: Configuration): Promise<CreateScopeResponse> {
        return this.api.readScope(param.scopeId,  options).toPromise();
    }

    /**
     * Update scope
     * Update scope
     * @param param the request object
     */
    public updateScope(param: ScopesApiUpdateScopeRequest, options?: Configuration): Promise<CreateScopeResponse> {
        return this.api.updateScope(param.scopeId, param.body,  options).toPromise();
    }

}

import { ObservableScriptApi } from "./ObservableAPI";
import { ScriptApiRequestFactory, ScriptApiResponseProcessor} from "../apis/ScriptApi";

export interface ScriptApiRunScriptRequest {
    /**
     * Name of the ledger.
     * @type string
     * @memberof ScriptApirunScript
     */
    ledger: string
    /**
     * 
     * @type Script
     * @memberof ScriptApirunScript
     */
    script: Script
    /**
     * Set the preview mode. Preview mode doesn&#39;t add the logs to the database or publish a message to the message broker.
     * @type boolean
     * @memberof ScriptApirunScript
     */
    preview?: boolean
}

export class ObjectScriptApi {
    private api: ObservableScriptApi

    public constructor(configuration: Configuration, requestFactory?: ScriptApiRequestFactory, responseProcessor?: ScriptApiResponseProcessor) {
        this.api = new ObservableScriptApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * This route is deprecated, and has been merged into `POST /{ledger}/transactions`. 
     * Execute a Numscript
     * @param param the request object
     */
    public runScript(param: ScriptApiRunScriptRequest, options?: Configuration): Promise<ScriptResponse> {
        return this.api.runScript(param.ledger, param.script, param.preview,  options).toPromise();
    }

}

import { ObservableSearchApi } from "./ObservableAPI";
import { SearchApiRequestFactory, SearchApiResponseProcessor} from "../apis/SearchApi";

export interface SearchApiSearchRequest {
    /**
     * 
     * @type Query
     * @memberof SearchApisearch
     */
    query: Query
}

export class ObjectSearchApi {
    private api: ObservableSearchApi

    public constructor(configuration: Configuration, requestFactory?: SearchApiRequestFactory, responseProcessor?: SearchApiResponseProcessor) {
        this.api = new ObservableSearchApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * ElasticSearch query engine
     * Search
     * @param param the request object
     */
    public search(param: SearchApiSearchRequest, options?: Configuration): Promise<Response> {
        return this.api.search(param.query,  options).toPromise();
    }

}

import { ObservableServerApi } from "./ObservableAPI";
import { ServerApiRequestFactory, ServerApiResponseProcessor} from "../apis/ServerApi";

export interface ServerApiGetInfoRequest {
}

export class ObjectServerApi {
    private api: ObservableServerApi

    public constructor(configuration: Configuration, requestFactory?: ServerApiRequestFactory, responseProcessor?: ServerApiResponseProcessor) {
        this.api = new ObservableServerApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Show server information
     * @param param the request object
     */
    public getInfo(param: ServerApiGetInfoRequest = {}, options?: Configuration): Promise<ConfigInfoResponse> {
        return this.api.getInfo( options).toPromise();
    }

}

import { ObservableStatsApi } from "./ObservableAPI";
import { StatsApiRequestFactory, StatsApiResponseProcessor} from "../apis/StatsApi";

export interface StatsApiReadStatsRequest {
    /**
     * name of the ledger
     * @type string
     * @memberof StatsApireadStats
     */
    ledger: string
}

export class ObjectStatsApi {
    private api: ObservableStatsApi

    public constructor(configuration: Configuration, requestFactory?: StatsApiRequestFactory, responseProcessor?: StatsApiResponseProcessor) {
        this.api = new ObservableStatsApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Get statistics from a ledger. (aggregate metrics on accounts and transactions) 
     * Get statistics from a ledger
     * @param param the request object
     */
    public readStats(param: StatsApiReadStatsRequest, options?: Configuration): Promise<StatsResponse> {
        return this.api.readStats(param.ledger,  options).toPromise();
    }

}

import { ObservableTransactionsApi } from "./ObservableAPI";
import { TransactionsApiRequestFactory, TransactionsApiResponseProcessor} from "../apis/TransactionsApi";

export interface TransactionsApiAddMetadataOnTransactionRequest {
    /**
     * Name of the ledger.
     * @type string
     * @memberof TransactionsApiaddMetadataOnTransaction
     */
    ledger: string
    /**
     * Transaction ID.
     * @type number
     * @memberof TransactionsApiaddMetadataOnTransaction
     */
    txid: number
    /**
     * metadata
     * @type { [key: string]: any; }
     * @memberof TransactionsApiaddMetadataOnTransaction
     */
    requestBody?: { [key: string]: any; }
}

export interface TransactionsApiCountTransactionsRequest {
    /**
     * Name of the ledger.
     * @type string
     * @memberof TransactionsApicountTransactions
     */
    ledger: string
    /**
     * Filter transactions by reference field.
     * @type string
     * @memberof TransactionsApicountTransactions
     */
    reference?: string
    /**
     * Filter transactions with postings involving given account, either as source or destination (regular expression placed between ^ and $).
     * @type string
     * @memberof TransactionsApicountTransactions
     */
    account?: string
    /**
     * Filter transactions with postings involving given account at source (regular expression placed between ^ and $).
     * @type string
     * @memberof TransactionsApicountTransactions
     */
    source?: string
    /**
     * Filter transactions with postings involving given account at destination (regular expression placed between ^ and $).
     * @type string
     * @memberof TransactionsApicountTransactions
     */
    destination?: string
    /**
     * Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). 
     * @type Date
     * @memberof TransactionsApicountTransactions
     */
    startTime?: Date
    /**
     * Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). Deprecated, please use &#x60;startTime&#x60; instead. 
     * @type Date
     * @memberof TransactionsApicountTransactions
     */
    startTime2?: Date
    /**
     * Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). 
     * @type Date
     * @memberof TransactionsApicountTransactions
     */
    endTime?: Date
    /**
     * Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). Deprecated, please use &#x60;endTime&#x60; instead. 
     * @type Date
     * @memberof TransactionsApicountTransactions
     */
    endTime2?: Date
    /**
     * Filter transactions by metadata key value pairs. Nested objects can be used as seen in the example below.
     * @type any
     * @memberof TransactionsApicountTransactions
     */
    metadata?: any
}

export interface TransactionsApiCreateTransactionRequest {
    /**
     * Name of the ledger.
     * @type string
     * @memberof TransactionsApicreateTransaction
     */
    ledger: string
    /**
     * The request body must contain at least one of the following objects:   - &#x60;postings&#x60;: suitable for simple transactions   - &#x60;script&#x60;: enabling more complex transactions with Numscript 
     * @type PostTransaction
     * @memberof TransactionsApicreateTransaction
     */
    postTransaction: PostTransaction
    /**
     * Set the preview mode. Preview mode doesn&#39;t add the logs to the database or publish a message to the message broker.
     * @type boolean
     * @memberof TransactionsApicreateTransaction
     */
    preview?: boolean
}

export interface TransactionsApiCreateTransactionsRequest {
    /**
     * Name of the ledger.
     * @type string
     * @memberof TransactionsApicreateTransactions
     */
    ledger: string
    /**
     * 
     * @type Transactions
     * @memberof TransactionsApicreateTransactions
     */
    transactions: Transactions
}

export interface TransactionsApiGetTransactionRequest {
    /**
     * Name of the ledger.
     * @type string
     * @memberof TransactionsApigetTransaction
     */
    ledger: string
    /**
     * Transaction ID.
     * @type number
     * @memberof TransactionsApigetTransaction
     */
    txid: number
}

export interface TransactionsApiListTransactionsRequest {
    /**
     * Name of the ledger.
     * @type string
     * @memberof TransactionsApilistTransactions
     */
    ledger: string
    /**
     * The maximum number of results to return per page. 
     * @type number
     * @memberof TransactionsApilistTransactions
     */
    pageSize?: number
    /**
     * The maximum number of results to return per page. Deprecated, please use &#x60;pageSize&#x60; instead. 
     * @type number
     * @memberof TransactionsApilistTransactions
     */
    pageSize2?: number
    /**
     * Pagination cursor, will return transactions after given txid (in descending order).
     * @type string
     * @memberof TransactionsApilistTransactions
     */
    after?: string
    /**
     * Find transactions by reference field.
     * @type string
     * @memberof TransactionsApilistTransactions
     */
    reference?: string
    /**
     * Filter transactions with postings involving given account, either as source or destination (regular expression placed between ^ and $).
     * @type string
     * @memberof TransactionsApilistTransactions
     */
    account?: string
    /**
     * Filter transactions with postings involving given account at source (regular expression placed between ^ and $).
     * @type string
     * @memberof TransactionsApilistTransactions
     */
    source?: string
    /**
     * Filter transactions with postings involving given account at destination (regular expression placed between ^ and $).
     * @type string
     * @memberof TransactionsApilistTransactions
     */
    destination?: string
    /**
     * Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). 
     * @type Date
     * @memberof TransactionsApilistTransactions
     */
    startTime?: Date
    /**
     * Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). Deprecated, please use &#x60;startTime&#x60; instead. 
     * @type Date
     * @memberof TransactionsApilistTransactions
     */
    startTime2?: Date
    /**
     * Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). 
     * @type Date
     * @memberof TransactionsApilistTransactions
     */
    endTime?: Date
    /**
     * Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). Deprecated, please use &#x60;endTime&#x60; instead. 
     * @type Date
     * @memberof TransactionsApilistTransactions
     */
    endTime2?: Date
    /**
     * Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
     * @type string
     * @memberof TransactionsApilistTransactions
     */
    cursor?: string
    /**
     * Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use &#x60;cursor&#x60; instead. 
     * @type string
     * @memberof TransactionsApilistTransactions
     */
    paginationToken?: string
    /**
     * Filter transactions by metadata key value pairs. Nested objects can be used as seen in the example below.
     * @type any
     * @memberof TransactionsApilistTransactions
     */
    metadata?: any
}

export interface TransactionsApiRevertTransactionRequest {
    /**
     * Name of the ledger.
     * @type string
     * @memberof TransactionsApirevertTransaction
     */
    ledger: string
    /**
     * Transaction ID.
     * @type number
     * @memberof TransactionsApirevertTransaction
     */
    txid: number
}

export class ObjectTransactionsApi {
    private api: ObservableTransactionsApi

    public constructor(configuration: Configuration, requestFactory?: TransactionsApiRequestFactory, responseProcessor?: TransactionsApiResponseProcessor) {
        this.api = new ObservableTransactionsApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Set the metadata of a transaction by its ID
     * @param param the request object
     */
    public addMetadataOnTransaction(param: TransactionsApiAddMetadataOnTransactionRequest, options?: Configuration): Promise<void> {
        return this.api.addMetadataOnTransaction(param.ledger, param.txid, param.requestBody,  options).toPromise();
    }

    /**
     * Count the transactions from a ledger
     * @param param the request object
     */
    public countTransactions(param: TransactionsApiCountTransactionsRequest, options?: Configuration): Promise<void> {
        return this.api.countTransactions(param.ledger, param.reference, param.account, param.source, param.destination, param.startTime, param.startTime2, param.endTime, param.endTime2, param.metadata,  options).toPromise();
    }

    /**
     * Create a new transaction to a ledger
     * @param param the request object
     */
    public createTransaction(param: TransactionsApiCreateTransactionRequest, options?: Configuration): Promise<TransactionsResponse> {
        return this.api.createTransaction(param.ledger, param.postTransaction, param.preview,  options).toPromise();
    }

    /**
     * Create a new batch of transactions to a ledger
     * @param param the request object
     */
    public createTransactions(param: TransactionsApiCreateTransactionsRequest, options?: Configuration): Promise<TransactionsResponse> {
        return this.api.createTransactions(param.ledger, param.transactions,  options).toPromise();
    }

    /**
     * Get transaction from a ledger by its ID
     * @param param the request object
     */
    public getTransaction(param: TransactionsApiGetTransactionRequest, options?: Configuration): Promise<TransactionResponse> {
        return this.api.getTransaction(param.ledger, param.txid,  options).toPromise();
    }

    /**
     * List transactions from a ledger, sorted by txid in descending order.
     * List transactions from a ledger
     * @param param the request object
     */
    public listTransactions(param: TransactionsApiListTransactionsRequest, options?: Configuration): Promise<TransactionsCursorResponse> {
        return this.api.listTransactions(param.ledger, param.pageSize, param.pageSize2, param.after, param.reference, param.account, param.source, param.destination, param.startTime, param.startTime2, param.endTime, param.endTime2, param.cursor, param.paginationToken, param.metadata,  options).toPromise();
    }

    /**
     * Revert a ledger transaction by its ID
     * @param param the request object
     */
    public revertTransaction(param: TransactionsApiRevertTransactionRequest, options?: Configuration): Promise<TransactionResponse> {
        return this.api.revertTransaction(param.ledger, param.txid,  options).toPromise();
    }

}

import { ObservableUsersApi } from "./ObservableAPI";
import { UsersApiRequestFactory, UsersApiResponseProcessor} from "../apis/UsersApi";

export interface UsersApiListUsersRequest {
}

export interface UsersApiReadUserRequest {
    /**
     * User ID
     * @type string
     * @memberof UsersApireadUser
     */
    userId: string
}

export class ObjectUsersApi {
    private api: ObservableUsersApi

    public constructor(configuration: Configuration, requestFactory?: UsersApiRequestFactory, responseProcessor?: UsersApiResponseProcessor) {
        this.api = new ObservableUsersApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * List users
     * List users
     * @param param the request object
     */
    public listUsers(param: UsersApiListUsersRequest = {}, options?: Configuration): Promise<ListUsersResponse> {
        return this.api.listUsers( options).toPromise();
    }

    /**
     * Read user
     * Read user
     * @param param the request object
     */
    public readUser(param: UsersApiReadUserRequest, options?: Configuration): Promise<ReadUserResponse> {
        return this.api.readUser(param.userId,  options).toPromise();
    }

}

import { ObservableWalletsApi } from "./ObservableAPI";
import { WalletsApiRequestFactory, WalletsApiResponseProcessor} from "../apis/WalletsApi";

export interface WalletsApiConfirmHoldRequest {
    /**
     * 
     * @type string
     * @memberof WalletsApiconfirmHold
     */
    holdId: string
    /**
     * 
     * @type ConfirmHoldRequest
     * @memberof WalletsApiconfirmHold
     */
    confirmHoldRequest?: ConfirmHoldRequest
}

export interface WalletsApiCreateBalanceRequest {
    /**
     * 
     * @type string
     * @memberof WalletsApicreateBalance
     */
    id: string
    /**
     * 
     * @type Balance
     * @memberof WalletsApicreateBalance
     */
    body?: Balance
}

export interface WalletsApiCreateWalletRequest {
    /**
     * 
     * @type CreateWalletRequest
     * @memberof WalletsApicreateWallet
     */
    createWalletRequest?: CreateWalletRequest
}

export interface WalletsApiCreditWalletRequest {
    /**
     * 
     * @type string
     * @memberof WalletsApicreditWallet
     */
    id: string
    /**
     * 
     * @type CreditWalletRequest
     * @memberof WalletsApicreditWallet
     */
    creditWalletRequest?: CreditWalletRequest
}

export interface WalletsApiDebitWalletRequest {
    /**
     * 
     * @type string
     * @memberof WalletsApidebitWallet
     */
    id: string
    /**
     * 
     * @type DebitWalletRequest
     * @memberof WalletsApidebitWallet
     */
    debitWalletRequest?: DebitWalletRequest
}

export interface WalletsApiGetBalanceRequest {
    /**
     * 
     * @type string
     * @memberof WalletsApigetBalance
     */
    id: string
    /**
     * 
     * @type string
     * @memberof WalletsApigetBalance
     */
    balanceName: string
}

export interface WalletsApiGetHoldRequest {
    /**
     * The hold ID
     * @type string
     * @memberof WalletsApigetHold
     */
    holdID: string
}

export interface WalletsApiGetHoldsRequest {
    /**
     * The maximum number of results to return per page
     * @type number
     * @memberof WalletsApigetHolds
     */
    pageSize?: number
    /**
     * The wallet to filter on
     * @type string
     * @memberof WalletsApigetHolds
     */
    walletID?: string
    /**
     * Filter holds by metadata key value pairs. Nested objects can be used as seen in the example below.
     * @type any
     * @memberof WalletsApigetHolds
     */
    metadata?: any
    /**
     * Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set. 
     * @type string
     * @memberof WalletsApigetHolds
     */
    cursor?: string
}

export interface WalletsApiGetTransactionsRequest {
    /**
     * The maximum number of results to return per page
     * @type number
     * @memberof WalletsApigetTransactions
     */
    pageSize?: number
    /**
     * A wallet ID to filter on
     * @type string
     * @memberof WalletsApigetTransactions
     */
    walletId?: string
    /**
     * Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the cursor is set. 
     * @type string
     * @memberof WalletsApigetTransactions
     */
    cursor?: string
}

export interface WalletsApiGetWalletRequest {
    /**
     * 
     * @type string
     * @memberof WalletsApigetWallet
     */
    id: string
}

export interface WalletsApiListBalancesRequest {
    /**
     * 
     * @type string
     * @memberof WalletsApilistBalances
     */
    id: string
}

export interface WalletsApiListWalletsRequest {
    /**
     * Filter on wallet name
     * @type string
     * @memberof WalletsApilistWallets
     */
    name?: string
    /**
     * Filter wallets by metadata key value pairs. Nested objects can be used as seen in the example below.
     * @type any
     * @memberof WalletsApilistWallets
     */
    metadata?: any
    /**
     * The maximum number of results to return per page
     * @type number
     * @memberof WalletsApilistWallets
     */
    pageSize?: number
    /**
     * Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set. 
     * @type string
     * @memberof WalletsApilistWallets
     */
    cursor?: string
}

export interface WalletsApiUpdateWalletRequest {
    /**
     * 
     * @type string
     * @memberof WalletsApiupdateWallet
     */
    id: string
    /**
     * 
     * @type UpdateWalletRequest
     * @memberof WalletsApiupdateWallet
     */
    updateWalletRequest?: UpdateWalletRequest
}

export interface WalletsApiVoidHoldRequest {
    /**
     * 
     * @type string
     * @memberof WalletsApivoidHold
     */
    holdId: string
}

export interface WalletsApiWalletsgetServerInfoRequest {
}

export class ObjectWalletsApi {
    private api: ObservableWalletsApi

    public constructor(configuration: Configuration, requestFactory?: WalletsApiRequestFactory, responseProcessor?: WalletsApiResponseProcessor) {
        this.api = new ObservableWalletsApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Confirm a hold
     * @param param the request object
     */
    public confirmHold(param: WalletsApiConfirmHoldRequest, options?: Configuration): Promise<void> {
        return this.api.confirmHold(param.holdId, param.confirmHoldRequest,  options).toPromise();
    }

    /**
     * Create a balance
     * @param param the request object
     */
    public createBalance(param: WalletsApiCreateBalanceRequest, options?: Configuration): Promise<CreateBalanceResponse> {
        return this.api.createBalance(param.id, param.body,  options).toPromise();
    }

    /**
     * Create a new wallet
     * @param param the request object
     */
    public createWallet(param: WalletsApiCreateWalletRequest = {}, options?: Configuration): Promise<CreateWalletResponse> {
        return this.api.createWallet(param.createWalletRequest,  options).toPromise();
    }

    /**
     * Credit a wallet
     * @param param the request object
     */
    public creditWallet(param: WalletsApiCreditWalletRequest, options?: Configuration): Promise<void> {
        return this.api.creditWallet(param.id, param.creditWalletRequest,  options).toPromise();
    }

    /**
     * Debit a wallet
     * @param param the request object
     */
    public debitWallet(param: WalletsApiDebitWalletRequest, options?: Configuration): Promise<DebitWalletResponse | void> {
        return this.api.debitWallet(param.id, param.debitWalletRequest,  options).toPromise();
    }

    /**
     * Get detailed balance
     * @param param the request object
     */
    public getBalance(param: WalletsApiGetBalanceRequest, options?: Configuration): Promise<GetBalanceResponse> {
        return this.api.getBalance(param.id, param.balanceName,  options).toPromise();
    }

    /**
     * Get a hold
     * @param param the request object
     */
    public getHold(param: WalletsApiGetHoldRequest, options?: Configuration): Promise<GetHoldResponse> {
        return this.api.getHold(param.holdID,  options).toPromise();
    }

    /**
     * Get all holds for a wallet
     * @param param the request object
     */
    public getHolds(param: WalletsApiGetHoldsRequest = {}, options?: Configuration): Promise<GetHoldsResponse> {
        return this.api.getHolds(param.pageSize, param.walletID, param.metadata, param.cursor,  options).toPromise();
    }

    /**
     * @param param the request object
     */
    public getTransactions(param: WalletsApiGetTransactionsRequest = {}, options?: Configuration): Promise<GetTransactionsResponse> {
        return this.api.getTransactions(param.pageSize, param.walletId, param.cursor,  options).toPromise();
    }

    /**
     * Get a wallet
     * @param param the request object
     */
    public getWallet(param: WalletsApiGetWalletRequest, options?: Configuration): Promise<GetWalletResponse> {
        return this.api.getWallet(param.id,  options).toPromise();
    }

    /**
     * List balances of a wallet
     * @param param the request object
     */
    public listBalances(param: WalletsApiListBalancesRequest, options?: Configuration): Promise<ListBalancesResponse> {
        return this.api.listBalances(param.id,  options).toPromise();
    }

    /**
     * List all wallets
     * @param param the request object
     */
    public listWallets(param: WalletsApiListWalletsRequest = {}, options?: Configuration): Promise<ListWalletsResponse> {
        return this.api.listWallets(param.name, param.metadata, param.pageSize, param.cursor,  options).toPromise();
    }

    /**
     * Update a wallet
     * @param param the request object
     */
    public updateWallet(param: WalletsApiUpdateWalletRequest, options?: Configuration): Promise<void> {
        return this.api.updateWallet(param.id, param.updateWalletRequest,  options).toPromise();
    }

    /**
     * Cancel a hold
     * @param param the request object
     */
    public voidHold(param: WalletsApiVoidHoldRequest, options?: Configuration): Promise<void> {
        return this.api.voidHold(param.holdId,  options).toPromise();
    }

    /**
     * Get server info
     * @param param the request object
     */
    public walletsgetServerInfo(param: WalletsApiWalletsgetServerInfoRequest = {}, options?: Configuration): Promise<ServerInfo> {
        return this.api.walletsgetServerInfo( options).toPromise();
    }

}

import { ObservableWebhooksApi } from "./ObservableAPI";
import { WebhooksApiRequestFactory, WebhooksApiResponseProcessor} from "../apis/WebhooksApi";

export interface WebhooksApiActivateConfigRequest {
    /**
     * Config ID
     * @type string
     * @memberof WebhooksApiactivateConfig
     */
    id: string
}

export interface WebhooksApiChangeConfigSecretRequest {
    /**
     * Config ID
     * @type string
     * @memberof WebhooksApichangeConfigSecret
     */
    id: string
    /**
     * 
     * @type ConfigChangeSecret
     * @memberof WebhooksApichangeConfigSecret
     */
    configChangeSecret?: ConfigChangeSecret
}

export interface WebhooksApiDeactivateConfigRequest {
    /**
     * Config ID
     * @type string
     * @memberof WebhooksApideactivateConfig
     */
    id: string
}

export interface WebhooksApiDeleteConfigRequest {
    /**
     * Config ID
     * @type string
     * @memberof WebhooksApideleteConfig
     */
    id: string
}

export interface WebhooksApiGetManyConfigsRequest {
    /**
     * Optional filter by Config ID
     * @type string
     * @memberof WebhooksApigetManyConfigs
     */
    id?: string
    /**
     * Optional filter by endpoint URL
     * @type string
     * @memberof WebhooksApigetManyConfigs
     */
    endpoint?: string
}

export interface WebhooksApiInsertConfigRequest {
    /**
     * 
     * @type ConfigUser
     * @memberof WebhooksApiinsertConfig
     */
    configUser: ConfigUser
}

export interface WebhooksApiTestConfigRequest {
    /**
     * Config ID
     * @type string
     * @memberof WebhooksApitestConfig
     */
    id: string
}

export class ObjectWebhooksApi {
    private api: ObservableWebhooksApi

    public constructor(configuration: Configuration, requestFactory?: WebhooksApiRequestFactory, responseProcessor?: WebhooksApiResponseProcessor) {
        this.api = new ObservableWebhooksApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Activate a webhooks config by ID, to start receiving webhooks to its endpoint.
     * Activate one config
     * @param param the request object
     */
    public activateConfig(param: WebhooksApiActivateConfigRequest, options?: Configuration): Promise<ConfigResponse> {
        return this.api.activateConfig(param.id,  options).toPromise();
    }

    /**
     * Change the signing secret of the endpoint of a webhooks config.  If not passed or empty, a secret is automatically generated. The format is a random string of bytes of size 24, base64 encoded. (larger size after encoding) 
     * Change the signing secret of a config
     * @param param the request object
     */
    public changeConfigSecret(param: WebhooksApiChangeConfigSecretRequest, options?: Configuration): Promise<ConfigResponse> {
        return this.api.changeConfigSecret(param.id, param.configChangeSecret,  options).toPromise();
    }

    /**
     * Deactivate a webhooks config by ID, to stop receiving webhooks to its endpoint.
     * Deactivate one config
     * @param param the request object
     */
    public deactivateConfig(param: WebhooksApiDeactivateConfigRequest, options?: Configuration): Promise<ConfigResponse> {
        return this.api.deactivateConfig(param.id,  options).toPromise();
    }

    /**
     * Delete a webhooks config by ID.
     * Delete one config
     * @param param the request object
     */
    public deleteConfig(param: WebhooksApiDeleteConfigRequest, options?: Configuration): Promise<void> {
        return this.api.deleteConfig(param.id,  options).toPromise();
    }

    /**
     * Sorted by updated date descending
     * Get many configs
     * @param param the request object
     */
    public getManyConfigs(param: WebhooksApiGetManyConfigsRequest = {}, options?: Configuration): Promise<ConfigsResponse> {
        return this.api.getManyConfigs(param.id, param.endpoint,  options).toPromise();
    }

    /**
     * Insert a new webhooks config.  The endpoint should be a valid https URL and be unique.  The secret is the endpoint's verification secret. If not passed or empty, a secret is automatically generated. The format is a random string of bytes of size 24, base64 encoded. (larger size after encoding)  All eventTypes are converted to lower-case when inserted. 
     * Insert a new config
     * @param param the request object
     */
    public insertConfig(param: WebhooksApiInsertConfigRequest, options?: Configuration): Promise<ConfigResponse> {
        return this.api.insertConfig(param.configUser,  options).toPromise();
    }

    /**
     * Test a config by sending a webhook to its endpoint.
     * Test one config
     * @param param the request object
     */
    public testConfig(param: WebhooksApiTestConfigRequest, options?: Configuration): Promise<AttemptResponse> {
        return this.api.testConfig(param.id,  options).toPromise();
    }

}
