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
import { OrchestrationCursor } from '../models/OrchestrationCursor';
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
import { WalletsCursor } from '../models/WalletsCursor';
import { WalletsErrorResponse } from '../models/WalletsErrorResponse';
import { WalletsTransaction } from '../models/WalletsTransaction';
import { WalletsVolume } from '../models/WalletsVolume';
import { WebhooksConfig } from '../models/WebhooksConfig';
import { WiseConfig } from '../models/WiseConfig';
import { Workflow } from '../models/Workflow';
import { WorkflowConfig } from '../models/WorkflowConfig';
import { WorkflowOccurrence } from '../models/WorkflowOccurrence';
import { ObservableAccountsApi } from './ObservableAPI';

import { AccountsApiRequestFactory, AccountsApiResponseProcessor} from "../apis/AccountsApi";
export class PromiseAccountsApi {
    private api: ObservableAccountsApi

    public constructor(
        configuration: Configuration,
        requestFactory?: AccountsApiRequestFactory,
        responseProcessor?: AccountsApiResponseProcessor
    ) {
        this.api = new ObservableAccountsApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Add metadata to an account
     * @param ledger Name of the ledger.
     * @param address Exact address of the account. It must match the following regular expressions pattern: &#x60;&#x60;&#x60; ^\\w+(:\\w+)*$ &#x60;&#x60;&#x60; 
     * @param requestBody metadata
     */
    public addMetadataToAccount(ledger: string, address: string, requestBody: { [key: string]: any; }, _options?: Configuration): Promise<void> {
        const result = this.api.addMetadataToAccount(ledger, address, requestBody, _options);
        return result.toPromise();
    }

    /**
     * Count the accounts from a ledger
     * @param ledger Name of the ledger.
     * @param address Filter accounts by address pattern (regular expression placed between ^ and $).
     * @param metadata Filter accounts by metadata key value pairs. Nested objects can be used as seen in the example below.
     */
    public countAccounts(ledger: string, address?: string, metadata?: any, _options?: Configuration): Promise<void> {
        const result = this.api.countAccounts(ledger, address, metadata, _options);
        return result.toPromise();
    }

    /**
     * Get account by its address
     * @param ledger Name of the ledger.
     * @param address Exact address of the account. It must match the following regular expressions pattern: &#x60;&#x60;&#x60; ^\\w+(:\\w+)*$ &#x60;&#x60;&#x60; 
     */
    public getAccount(ledger: string, address: string, _options?: Configuration): Promise<AccountResponse> {
        const result = this.api.getAccount(ledger, address, _options);
        return result.toPromise();
    }

    /**
     * List accounts from a ledger, sorted by address in descending order.
     * List accounts from a ledger
     * @param ledger Name of the ledger.
     * @param pageSize The maximum number of results to return per page. 
     * @param pageSize2 The maximum number of results to return per page. Deprecated, please use &#x60;pageSize&#x60; instead. 
     * @param after Pagination cursor, will return accounts after given address, in descending order.
     * @param address Filter accounts by address pattern (regular expression placed between ^ and $).
     * @param metadata Filter accounts by metadata key value pairs. Nested objects can be used as seen in the example below.
     * @param balance Filter accounts by their balance (default operator is gte)
     * @param balanceOperator Operator used for the filtering of balances can be greater than/equal, less than/equal, greater than, less than, equal or not. 
     * @param balanceOperator2 Operator used for the filtering of balances can be greater than/equal, less than/equal, greater than, less than, equal or not. Deprecated, please use &#x60;balanceOperator&#x60; instead. 
     * @param cursor Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
     * @param paginationToken Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use &#x60;cursor&#x60; instead. 
     */
    public listAccounts(ledger: string, pageSize?: number, pageSize2?: number, after?: string, address?: string, metadata?: any, balance?: number, balanceOperator?: 'gte' | 'lte' | 'gt' | 'lt' | 'e' | 'ne', balanceOperator2?: 'gte' | 'lte' | 'gt' | 'lt' | 'e' | 'ne', cursor?: string, paginationToken?: string, _options?: Configuration): Promise<AccountsCursorResponse> {
        const result = this.api.listAccounts(ledger, pageSize, pageSize2, after, address, metadata, balance, balanceOperator, balanceOperator2, cursor, paginationToken, _options);
        return result.toPromise();
    }


}



import { ObservableBalancesApi } from './ObservableAPI';

import { BalancesApiRequestFactory, BalancesApiResponseProcessor} from "../apis/BalancesApi";
export class PromiseBalancesApi {
    private api: ObservableBalancesApi

    public constructor(
        configuration: Configuration,
        requestFactory?: BalancesApiRequestFactory,
        responseProcessor?: BalancesApiResponseProcessor
    ) {
        this.api = new ObservableBalancesApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Get the balances from a ledger's account
     * @param ledger Name of the ledger.
     * @param address Filter balances involving given account, either as source or destination.
     * @param after Pagination cursor, will return accounts after given address, in descending order.
     * @param cursor Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
     * @param paginationToken Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. Deprecated, please use &#x60;cursor&#x60; instead.
     */
    public getBalances(ledger: string, address?: string, after?: string, cursor?: string, paginationToken?: string, _options?: Configuration): Promise<BalancesCursorResponse> {
        const result = this.api.getBalances(ledger, address, after, cursor, paginationToken, _options);
        return result.toPromise();
    }

    /**
     * Get the aggregated balances from selected accounts
     * @param ledger Name of the ledger.
     * @param address Filter balances involving given account, either as source or destination.
     */
    public getBalancesAggregated(ledger: string, address?: string, _options?: Configuration): Promise<AggregateBalancesResponse> {
        const result = this.api.getBalancesAggregated(ledger, address, _options);
        return result.toPromise();
    }


}



import { ObservableClientsApi } from './ObservableAPI';

import { ClientsApiRequestFactory, ClientsApiResponseProcessor} from "../apis/ClientsApi";
export class PromiseClientsApi {
    private api: ObservableClientsApi

    public constructor(
        configuration: Configuration,
        requestFactory?: ClientsApiRequestFactory,
        responseProcessor?: ClientsApiResponseProcessor
    ) {
        this.api = new ObservableClientsApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Add scope to client
     * @param clientId Client ID
     * @param scopeId Scope ID
     */
    public addScopeToClient(clientId: string, scopeId: string, _options?: Configuration): Promise<void> {
        const result = this.api.addScopeToClient(clientId, scopeId, _options);
        return result.toPromise();
    }

    /**
     * Create client
     * @param body 
     */
    public createClient(body?: ClientOptions, _options?: Configuration): Promise<CreateClientResponse> {
        const result = this.api.createClient(body, _options);
        return result.toPromise();
    }

    /**
     * Add a secret to a client
     * @param clientId Client ID
     * @param body 
     */
    public createSecret(clientId: string, body?: SecretOptions, _options?: Configuration): Promise<CreateSecretResponse> {
        const result = this.api.createSecret(clientId, body, _options);
        return result.toPromise();
    }

    /**
     * Delete client
     * @param clientId Client ID
     */
    public deleteClient(clientId: string, _options?: Configuration): Promise<void> {
        const result = this.api.deleteClient(clientId, _options);
        return result.toPromise();
    }

    /**
     * Delete scope from client
     * @param clientId Client ID
     * @param scopeId Scope ID
     */
    public deleteScopeFromClient(clientId: string, scopeId: string, _options?: Configuration): Promise<void> {
        const result = this.api.deleteScopeFromClient(clientId, scopeId, _options);
        return result.toPromise();
    }

    /**
     * Delete a secret from a client
     * @param clientId Client ID
     * @param secretId Secret ID
     */
    public deleteSecret(clientId: string, secretId: string, _options?: Configuration): Promise<void> {
        const result = this.api.deleteSecret(clientId, secretId, _options);
        return result.toPromise();
    }

    /**
     * List clients
     */
    public listClients(_options?: Configuration): Promise<ListClientsResponse> {
        const result = this.api.listClients(_options);
        return result.toPromise();
    }

    /**
     * Read client
     * @param clientId Client ID
     */
    public readClient(clientId: string, _options?: Configuration): Promise<ReadClientResponse> {
        const result = this.api.readClient(clientId, _options);
        return result.toPromise();
    }

    /**
     * Update client
     * @param clientId Client ID
     * @param body 
     */
    public updateClient(clientId: string, body?: ClientOptions, _options?: Configuration): Promise<CreateClientResponse> {
        const result = this.api.updateClient(clientId, body, _options);
        return result.toPromise();
    }


}



import { ObservableDefaultApi } from './ObservableAPI';

import { DefaultApiRequestFactory, DefaultApiResponseProcessor} from "../apis/DefaultApi";
export class PromiseDefaultApi {
    private api: ObservableDefaultApi

    public constructor(
        configuration: Configuration,
        requestFactory?: DefaultApiRequestFactory,
        responseProcessor?: DefaultApiResponseProcessor
    ) {
        this.api = new ObservableDefaultApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Get server info
     */
    public getServerInfo(_options?: Configuration): Promise<ServerInfo> {
        const result = this.api.getServerInfo(_options);
        return result.toPromise();
    }

    /**
     * Get server info
     */
    public paymentsgetServerInfo(_options?: Configuration): Promise<ServerInfo> {
        const result = this.api.paymentsgetServerInfo(_options);
        return result.toPromise();
    }

    /**
     * Get server info
     */
    public searchgetServerInfo(_options?: Configuration): Promise<ServerInfo> {
        const result = this.api.searchgetServerInfo(_options);
        return result.toPromise();
    }


}



import { ObservableLedgerApi } from './ObservableAPI';

import { LedgerApiRequestFactory, LedgerApiResponseProcessor} from "../apis/LedgerApi";
export class PromiseLedgerApi {
    private api: ObservableLedgerApi

    public constructor(
        configuration: Configuration,
        requestFactory?: LedgerApiRequestFactory,
        responseProcessor?: LedgerApiResponseProcessor
    ) {
        this.api = new ObservableLedgerApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Get information about a ledger
     * @param ledger Name of the ledger.
     */
    public getLedgerInfo(ledger: string, _options?: Configuration): Promise<LedgerInfoResponse> {
        const result = this.api.getLedgerInfo(ledger, _options);
        return result.toPromise();
    }


}



import { ObservableLogsApi } from './ObservableAPI';

import { LogsApiRequestFactory, LogsApiResponseProcessor} from "../apis/LogsApi";
export class PromiseLogsApi {
    private api: ObservableLogsApi

    public constructor(
        configuration: Configuration,
        requestFactory?: LogsApiRequestFactory,
        responseProcessor?: LogsApiResponseProcessor
    ) {
        this.api = new ObservableLogsApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * List the logs from a ledger, sorted by ID in descending order.
     * List the logs from a ledger
     * @param ledger Name of the ledger.
     * @param pageSize The maximum number of results to return per page. 
     * @param pageSize2 The maximum number of results to return per page. Deprecated, please use &#x60;pageSize&#x60; instead. 
     * @param after Pagination cursor, will return the logs after a given ID. (in descending order).
     * @param startTime Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). 
     * @param startTime2 Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). Deprecated, please use &#x60;startTime&#x60; instead. 
     * @param endTime Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). 
     * @param endTime2 Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). Deprecated, please use &#x60;endTime&#x60; instead. 
     * @param cursor Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
     * @param paginationToken Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use &#x60;cursor&#x60; instead. 
     */
    public listLogs(ledger: string, pageSize?: number, pageSize2?: number, after?: string, startTime?: Date, startTime2?: Date, endTime?: Date, endTime2?: Date, cursor?: string, paginationToken?: string, _options?: Configuration): Promise<LogsCursorResponse> {
        const result = this.api.listLogs(ledger, pageSize, pageSize2, after, startTime, startTime2, endTime, endTime2, cursor, paginationToken, _options);
        return result.toPromise();
    }


}



import { ObservableMappingApi } from './ObservableAPI';

import { MappingApiRequestFactory, MappingApiResponseProcessor} from "../apis/MappingApi";
export class PromiseMappingApi {
    private api: ObservableMappingApi

    public constructor(
        configuration: Configuration,
        requestFactory?: MappingApiRequestFactory,
        responseProcessor?: MappingApiResponseProcessor
    ) {
        this.api = new ObservableMappingApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Get the mapping of a ledger
     * @param ledger Name of the ledger.
     */
    public getMapping(ledger: string, _options?: Configuration): Promise<MappingResponse> {
        const result = this.api.getMapping(ledger, _options);
        return result.toPromise();
    }

    /**
     * Update the mapping of a ledger
     * @param ledger Name of the ledger.
     * @param mapping 
     */
    public updateMapping(ledger: string, mapping: Mapping, _options?: Configuration): Promise<MappingResponse> {
        const result = this.api.updateMapping(ledger, mapping, _options);
        return result.toPromise();
    }


}



import { ObservableOrchestrationApi } from './ObservableAPI';

import { OrchestrationApiRequestFactory, OrchestrationApiResponseProcessor} from "../apis/OrchestrationApi";
export class PromiseOrchestrationApi {
    private api: ObservableOrchestrationApi

    public constructor(
        configuration: Configuration,
        requestFactory?: OrchestrationApiRequestFactory,
        responseProcessor?: OrchestrationApiResponseProcessor
    ) {
        this.api = new ObservableOrchestrationApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Create a workflow
     * Create workflow
     * @param body 
     */
    public createWorkflow(body?: WorkflowConfig, _options?: Configuration): Promise<CreateWorkflowResponse> {
        const result = this.api.createWorkflow(body, _options);
        return result.toPromise();
    }

    /**
     * Get a flow by id
     * Get a flow by id
     * @param flowId The flow id
     */
    public getFlow(flowId: string, _options?: Configuration): Promise<GetWorkflowResponse> {
        const result = this.api.getFlow(flowId, _options);
        return result.toPromise();
    }

    /**
     * Get a workflow occurrence by id
     * Get a workflow occurrence by id
     * @param flowId The flow id
     * @param runId The occurrence id
     */
    public getWorkflowOccurrence(flowId: string, runId: string, _options?: Configuration): Promise<GetWorkflowOccurrenceResponse> {
        const result = this.api.getWorkflowOccurrence(flowId, runId, _options);
        return result.toPromise();
    }

    /**
     * List registered flows
     * List registered flows
     */
    public listFlows(_options?: Configuration): Promise<ListWorkflowsResponse> {
        const result = this.api.listFlows(_options);
        return result.toPromise();
    }

    /**
     * List occurrences of a workflow
     * List occurrences of a workflow
     * @param flowId The flow id
     */
    public listRuns(flowId: string, _options?: Configuration): Promise<ListRunsResponse> {
        const result = this.api.listRuns(flowId, _options);
        return result.toPromise();
    }

    /**
     * Get server info
     */
    public orchestrationgetServerInfo(_options?: Configuration): Promise<ServerInfo> {
        const result = this.api.orchestrationgetServerInfo(_options);
        return result.toPromise();
    }

    /**
     * Run workflow
     * Run workflow
     * @param flowId The flow id
     * @param wait Wait end of the workflow before return
     * @param requestBody 
     */
    public runWorkflow(flowId: string, wait?: boolean, requestBody?: { [key: string]: string; }, _options?: Configuration): Promise<RunWorkflowResponse> {
        const result = this.api.runWorkflow(flowId, wait, requestBody, _options);
        return result.toPromise();
    }


}



import { ObservablePaymentsApi } from './ObservableAPI';

import { PaymentsApiRequestFactory, PaymentsApiResponseProcessor} from "../apis/PaymentsApi";
export class PromisePaymentsApi {
    private api: ObservablePaymentsApi

    public constructor(
        configuration: Configuration,
        requestFactory?: PaymentsApiRequestFactory,
        responseProcessor?: PaymentsApiResponseProcessor
    ) {
        this.api = new ObservablePaymentsApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Execute a transfer between two Stripe accounts.
     * Transfer funds between Stripe accounts
     * @param stripeTransferRequest 
     */
    public connectorsStripeTransfer(stripeTransferRequest: StripeTransferRequest, _options?: Configuration): Promise<any> {
        const result = this.api.connectorsStripeTransfer(stripeTransferRequest, _options);
        return result.toPromise();
    }

    /**
     * Get a specific task associated to the connector.
     * Read a specific task of the connector
     * @param connector The name of the connector.
     * @param taskId The task ID.
     */
    public getConnectorTask(connector: Connector, taskId: string, _options?: Configuration): Promise<TaskResponse> {
        const result = this.api.getConnectorTask(connector, taskId, _options);
        return result.toPromise();
    }

    /**
     * Get a payment
     * @param paymentId The payment ID.
     */
    public getPayment(paymentId: string, _options?: Configuration): Promise<PaymentResponse> {
        const result = this.api.getPayment(paymentId, _options);
        return result.toPromise();
    }

    /**
     * Install a connector by its name and config.
     * Install a connector
     * @param connector The name of the connector.
     * @param connectorConfig 
     */
    public installConnector(connector: Connector, connectorConfig: ConnectorConfig, _options?: Configuration): Promise<void> {
        const result = this.api.installConnector(connector, connectorConfig, _options);
        return result.toPromise();
    }

    /**
     * List all installed connectors.
     * List all installed connectors
     */
    public listAllConnectors(_options?: Configuration): Promise<ConnectorsResponse> {
        const result = this.api.listAllConnectors(_options);
        return result.toPromise();
    }

    /**
     * List the configs of each available connector.
     * List the configs of each available connector
     */
    public listConfigsAvailableConnectors(_options?: Configuration): Promise<ConnectorsConfigsResponse> {
        const result = this.api.listConfigsAvailableConnectors(_options);
        return result.toPromise();
    }

    /**
     * List all tasks associated with this connector.
     * List tasks from a connector
     * @param connector The name of the connector.
     * @param pageSize The maximum number of results to return per page. 
     * @param cursor Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
     */
    public listConnectorTasks(connector: Connector, pageSize?: number, cursor?: string, _options?: Configuration): Promise<TasksCursor> {
        const result = this.api.listConnectorTasks(connector, pageSize, cursor, _options);
        return result.toPromise();
    }

    /**
     * List payments
     * @param pageSize The maximum number of results to return per page. 
     * @param cursor Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
     * @param sort Fields used to sort payments (default is date:desc).
     */
    public listPayments(pageSize?: number, cursor?: string, sort?: Array<string>, _options?: Configuration): Promise<PaymentsCursor> {
        const result = this.api.listPayments(pageSize, cursor, sort, _options);
        return result.toPromise();
    }

    /**
     * List accounts
     * @param pageSize The maximum number of results to return per page. 
     * @param cursor Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
     * @param sort Fields used to sort payments (default is date:desc).
     */
    public paymentslistAccounts(pageSize?: number, cursor?: string, sort?: Array<string>, _options?: Configuration): Promise<AccountsCursor> {
        const result = this.api.paymentslistAccounts(pageSize, cursor, sort, _options);
        return result.toPromise();
    }

    /**
     * Read connector config
     * Read the config of a connector
     * @param connector The name of the connector.
     */
    public readConnectorConfig(connector: Connector, _options?: Configuration): Promise<ConnectorConfigResponse> {
        const result = this.api.readConnectorConfig(connector, _options);
        return result.toPromise();
    }

    /**
     * Reset a connector by its name. It will remove the connector and ALL PAYMENTS generated with it. 
     * Reset a connector
     * @param connector The name of the connector.
     */
    public resetConnector(connector: Connector, _options?: Configuration): Promise<void> {
        const result = this.api.resetConnector(connector, _options);
        return result.toPromise();
    }

    /**
     * Uninstall a connector by its name.
     * Uninstall a connector
     * @param connector The name of the connector.
     */
    public uninstallConnector(connector: Connector, _options?: Configuration): Promise<void> {
        const result = this.api.uninstallConnector(connector, _options);
        return result.toPromise();
    }


}



import { ObservableScopesApi } from './ObservableAPI';

import { ScopesApiRequestFactory, ScopesApiResponseProcessor} from "../apis/ScopesApi";
export class PromiseScopesApi {
    private api: ObservableScopesApi

    public constructor(
        configuration: Configuration,
        requestFactory?: ScopesApiRequestFactory,
        responseProcessor?: ScopesApiResponseProcessor
    ) {
        this.api = new ObservableScopesApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Add a transient scope to a scope
     * Add a transient scope to a scope
     * @param scopeId Scope ID
     * @param transientScopeId Transient scope ID
     */
    public addTransientScope(scopeId: string, transientScopeId: string, _options?: Configuration): Promise<void> {
        const result = this.api.addTransientScope(scopeId, transientScopeId, _options);
        return result.toPromise();
    }

    /**
     * Create scope
     * Create scope
     * @param body 
     */
    public createScope(body?: ScopeOptions, _options?: Configuration): Promise<CreateScopeResponse> {
        const result = this.api.createScope(body, _options);
        return result.toPromise();
    }

    /**
     * Delete scope
     * Delete scope
     * @param scopeId Scope ID
     */
    public deleteScope(scopeId: string, _options?: Configuration): Promise<void> {
        const result = this.api.deleteScope(scopeId, _options);
        return result.toPromise();
    }

    /**
     * Delete a transient scope from a scope
     * Delete a transient scope from a scope
     * @param scopeId Scope ID
     * @param transientScopeId Transient scope ID
     */
    public deleteTransientScope(scopeId: string, transientScopeId: string, _options?: Configuration): Promise<void> {
        const result = this.api.deleteTransientScope(scopeId, transientScopeId, _options);
        return result.toPromise();
    }

    /**
     * List Scopes
     * List scopes
     */
    public listScopes(_options?: Configuration): Promise<ListScopesResponse> {
        const result = this.api.listScopes(_options);
        return result.toPromise();
    }

    /**
     * Read scope
     * Read scope
     * @param scopeId Scope ID
     */
    public readScope(scopeId: string, _options?: Configuration): Promise<CreateScopeResponse> {
        const result = this.api.readScope(scopeId, _options);
        return result.toPromise();
    }

    /**
     * Update scope
     * Update scope
     * @param scopeId Scope ID
     * @param body 
     */
    public updateScope(scopeId: string, body?: ScopeOptions, _options?: Configuration): Promise<CreateScopeResponse> {
        const result = this.api.updateScope(scopeId, body, _options);
        return result.toPromise();
    }


}



import { ObservableScriptApi } from './ObservableAPI';

import { ScriptApiRequestFactory, ScriptApiResponseProcessor} from "../apis/ScriptApi";
export class PromiseScriptApi {
    private api: ObservableScriptApi

    public constructor(
        configuration: Configuration,
        requestFactory?: ScriptApiRequestFactory,
        responseProcessor?: ScriptApiResponseProcessor
    ) {
        this.api = new ObservableScriptApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * This route is deprecated, and has been merged into `POST /{ledger}/transactions`. 
     * Execute a Numscript
     * @param ledger Name of the ledger.
     * @param script 
     * @param preview Set the preview mode. Preview mode doesn&#39;t add the logs to the database or publish a message to the message broker.
     */
    public runScript(ledger: string, script: Script, preview?: boolean, _options?: Configuration): Promise<ScriptResponse> {
        const result = this.api.runScript(ledger, script, preview, _options);
        return result.toPromise();
    }


}



import { ObservableSearchApi } from './ObservableAPI';

import { SearchApiRequestFactory, SearchApiResponseProcessor} from "../apis/SearchApi";
export class PromiseSearchApi {
    private api: ObservableSearchApi

    public constructor(
        configuration: Configuration,
        requestFactory?: SearchApiRequestFactory,
        responseProcessor?: SearchApiResponseProcessor
    ) {
        this.api = new ObservableSearchApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * ElasticSearch query engine
     * Search
     * @param query 
     */
    public search(query: Query, _options?: Configuration): Promise<Response> {
        const result = this.api.search(query, _options);
        return result.toPromise();
    }


}



import { ObservableServerApi } from './ObservableAPI';

import { ServerApiRequestFactory, ServerApiResponseProcessor} from "../apis/ServerApi";
export class PromiseServerApi {
    private api: ObservableServerApi

    public constructor(
        configuration: Configuration,
        requestFactory?: ServerApiRequestFactory,
        responseProcessor?: ServerApiResponseProcessor
    ) {
        this.api = new ObservableServerApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Show server information
     */
    public getInfo(_options?: Configuration): Promise<ConfigInfoResponse> {
        const result = this.api.getInfo(_options);
        return result.toPromise();
    }


}



import { ObservableStatsApi } from './ObservableAPI';

import { StatsApiRequestFactory, StatsApiResponseProcessor} from "../apis/StatsApi";
export class PromiseStatsApi {
    private api: ObservableStatsApi

    public constructor(
        configuration: Configuration,
        requestFactory?: StatsApiRequestFactory,
        responseProcessor?: StatsApiResponseProcessor
    ) {
        this.api = new ObservableStatsApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Get statistics from a ledger. (aggregate metrics on accounts and transactions) 
     * Get statistics from a ledger
     * @param ledger name of the ledger
     */
    public readStats(ledger: string, _options?: Configuration): Promise<StatsResponse> {
        const result = this.api.readStats(ledger, _options);
        return result.toPromise();
    }


}



import { ObservableTransactionsApi } from './ObservableAPI';

import { TransactionsApiRequestFactory, TransactionsApiResponseProcessor} from "../apis/TransactionsApi";
export class PromiseTransactionsApi {
    private api: ObservableTransactionsApi

    public constructor(
        configuration: Configuration,
        requestFactory?: TransactionsApiRequestFactory,
        responseProcessor?: TransactionsApiResponseProcessor
    ) {
        this.api = new ObservableTransactionsApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Set the metadata of a transaction by its ID
     * @param ledger Name of the ledger.
     * @param txid Transaction ID.
     * @param requestBody metadata
     */
    public addMetadataOnTransaction(ledger: string, txid: number, requestBody?: { [key: string]: any; }, _options?: Configuration): Promise<void> {
        const result = this.api.addMetadataOnTransaction(ledger, txid, requestBody, _options);
        return result.toPromise();
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
    public countTransactions(ledger: string, reference?: string, account?: string, source?: string, destination?: string, startTime?: Date, startTime2?: Date, endTime?: Date, endTime2?: Date, metadata?: any, _options?: Configuration): Promise<void> {
        const result = this.api.countTransactions(ledger, reference, account, source, destination, startTime, startTime2, endTime, endTime2, metadata, _options);
        return result.toPromise();
    }

    /**
     * Create a new transaction to a ledger
     * @param ledger Name of the ledger.
     * @param postTransaction The request body must contain at least one of the following objects:   - &#x60;postings&#x60;: suitable for simple transactions   - &#x60;script&#x60;: enabling more complex transactions with Numscript 
     * @param preview Set the preview mode. Preview mode doesn&#39;t add the logs to the database or publish a message to the message broker.
     */
    public createTransaction(ledger: string, postTransaction: PostTransaction, preview?: boolean, _options?: Configuration): Promise<TransactionsResponse> {
        const result = this.api.createTransaction(ledger, postTransaction, preview, _options);
        return result.toPromise();
    }

    /**
     * Create a new batch of transactions to a ledger
     * @param ledger Name of the ledger.
     * @param transactions 
     */
    public createTransactions(ledger: string, transactions: Transactions, _options?: Configuration): Promise<TransactionsResponse> {
        const result = this.api.createTransactions(ledger, transactions, _options);
        return result.toPromise();
    }

    /**
     * Get transaction from a ledger by its ID
     * @param ledger Name of the ledger.
     * @param txid Transaction ID.
     */
    public getTransaction(ledger: string, txid: number, _options?: Configuration): Promise<TransactionResponse> {
        const result = this.api.getTransaction(ledger, txid, _options);
        return result.toPromise();
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
    public listTransactions(ledger: string, pageSize?: number, pageSize2?: number, after?: string, reference?: string, account?: string, source?: string, destination?: string, startTime?: Date, startTime2?: Date, endTime?: Date, endTime2?: Date, cursor?: string, paginationToken?: string, metadata?: any, _options?: Configuration): Promise<TransactionsCursorResponse> {
        const result = this.api.listTransactions(ledger, pageSize, pageSize2, after, reference, account, source, destination, startTime, startTime2, endTime, endTime2, cursor, paginationToken, metadata, _options);
        return result.toPromise();
    }

    /**
     * Revert a ledger transaction by its ID
     * @param ledger Name of the ledger.
     * @param txid Transaction ID.
     */
    public revertTransaction(ledger: string, txid: number, _options?: Configuration): Promise<TransactionResponse> {
        const result = this.api.revertTransaction(ledger, txid, _options);
        return result.toPromise();
    }


}



import { ObservableUsersApi } from './ObservableAPI';

import { UsersApiRequestFactory, UsersApiResponseProcessor} from "../apis/UsersApi";
export class PromiseUsersApi {
    private api: ObservableUsersApi

    public constructor(
        configuration: Configuration,
        requestFactory?: UsersApiRequestFactory,
        responseProcessor?: UsersApiResponseProcessor
    ) {
        this.api = new ObservableUsersApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * List users
     * List users
     */
    public listUsers(_options?: Configuration): Promise<ListUsersResponse> {
        const result = this.api.listUsers(_options);
        return result.toPromise();
    }

    /**
     * Read user
     * Read user
     * @param userId User ID
     */
    public readUser(userId: string, _options?: Configuration): Promise<ReadUserResponse> {
        const result = this.api.readUser(userId, _options);
        return result.toPromise();
    }


}



import { ObservableWalletsApi } from './ObservableAPI';

import { WalletsApiRequestFactory, WalletsApiResponseProcessor} from "../apis/WalletsApi";
export class PromiseWalletsApi {
    private api: ObservableWalletsApi

    public constructor(
        configuration: Configuration,
        requestFactory?: WalletsApiRequestFactory,
        responseProcessor?: WalletsApiResponseProcessor
    ) {
        this.api = new ObservableWalletsApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Confirm a hold
     * @param holdId 
     * @param confirmHoldRequest 
     */
    public confirmHold(holdId: string, confirmHoldRequest?: ConfirmHoldRequest, _options?: Configuration): Promise<void> {
        const result = this.api.confirmHold(holdId, confirmHoldRequest, _options);
        return result.toPromise();
    }

    /**
     * Create a balance
     * @param id 
     * @param body 
     */
    public createBalance(id: string, body?: Balance, _options?: Configuration): Promise<CreateBalanceResponse> {
        const result = this.api.createBalance(id, body, _options);
        return result.toPromise();
    }

    /**
     * Create a new wallet
     * @param createWalletRequest 
     */
    public createWallet(createWalletRequest?: CreateWalletRequest, _options?: Configuration): Promise<CreateWalletResponse> {
        const result = this.api.createWallet(createWalletRequest, _options);
        return result.toPromise();
    }

    /**
     * Credit a wallet
     * @param id 
     * @param creditWalletRequest 
     */
    public creditWallet(id: string, creditWalletRequest?: CreditWalletRequest, _options?: Configuration): Promise<void> {
        const result = this.api.creditWallet(id, creditWalletRequest, _options);
        return result.toPromise();
    }

    /**
     * Debit a wallet
     * @param id 
     * @param debitWalletRequest 
     */
    public debitWallet(id: string, debitWalletRequest?: DebitWalletRequest, _options?: Configuration): Promise<DebitWalletResponse | void> {
        const result = this.api.debitWallet(id, debitWalletRequest, _options);
        return result.toPromise();
    }

    /**
     * Get detailed balance
     * @param id 
     * @param balanceName 
     */
    public getBalance(id: string, balanceName: string, _options?: Configuration): Promise<GetBalanceResponse> {
        const result = this.api.getBalance(id, balanceName, _options);
        return result.toPromise();
    }

    /**
     * Get a hold
     * @param holdID The hold ID
     */
    public getHold(holdID: string, _options?: Configuration): Promise<GetHoldResponse> {
        const result = this.api.getHold(holdID, _options);
        return result.toPromise();
    }

    /**
     * Get all holds for a wallet
     * @param pageSize The maximum number of results to return per page
     * @param walletID The wallet to filter on
     * @param metadata Filter holds by metadata key value pairs. Nested objects can be used as seen in the example below.
     * @param cursor Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set. 
     */
    public getHolds(pageSize?: number, walletID?: string, metadata?: any, cursor?: string, _options?: Configuration): Promise<GetHoldsResponse> {
        const result = this.api.getHolds(pageSize, walletID, metadata, cursor, _options);
        return result.toPromise();
    }

    /**
     * @param pageSize The maximum number of results to return per page
     * @param walletId A wallet ID to filter on
     * @param cursor Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the cursor is set. 
     */
    public getTransactions(pageSize?: number, walletId?: string, cursor?: string, _options?: Configuration): Promise<GetTransactionsResponse> {
        const result = this.api.getTransactions(pageSize, walletId, cursor, _options);
        return result.toPromise();
    }

    /**
     * Get a wallet
     * @param id 
     */
    public getWallet(id: string, _options?: Configuration): Promise<GetWalletResponse> {
        const result = this.api.getWallet(id, _options);
        return result.toPromise();
    }

    /**
     * List balances of a wallet
     * @param id 
     */
    public listBalances(id: string, _options?: Configuration): Promise<ListBalancesResponse> {
        const result = this.api.listBalances(id, _options);
        return result.toPromise();
    }

    /**
     * List all wallets
     * @param name Filter on wallet name
     * @param metadata Filter wallets by metadata key value pairs. Nested objects can be used as seen in the example below.
     * @param pageSize The maximum number of results to return per page
     * @param cursor Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set. 
     */
    public listWallets(name?: string, metadata?: any, pageSize?: number, cursor?: string, _options?: Configuration): Promise<ListWalletsResponse> {
        const result = this.api.listWallets(name, metadata, pageSize, cursor, _options);
        return result.toPromise();
    }

    /**
     * Update a wallet
     * @param id 
     * @param updateWalletRequest 
     */
    public updateWallet(id: string, updateWalletRequest?: UpdateWalletRequest, _options?: Configuration): Promise<void> {
        const result = this.api.updateWallet(id, updateWalletRequest, _options);
        return result.toPromise();
    }

    /**
     * Cancel a hold
     * @param holdId 
     */
    public voidHold(holdId: string, _options?: Configuration): Promise<void> {
        const result = this.api.voidHold(holdId, _options);
        return result.toPromise();
    }

    /**
     * Get server info
     */
    public walletsgetServerInfo(_options?: Configuration): Promise<ServerInfo> {
        const result = this.api.walletsgetServerInfo(_options);
        return result.toPromise();
    }


}



import { ObservableWebhooksApi } from './ObservableAPI';

import { WebhooksApiRequestFactory, WebhooksApiResponseProcessor} from "../apis/WebhooksApi";
export class PromiseWebhooksApi {
    private api: ObservableWebhooksApi

    public constructor(
        configuration: Configuration,
        requestFactory?: WebhooksApiRequestFactory,
        responseProcessor?: WebhooksApiResponseProcessor
    ) {
        this.api = new ObservableWebhooksApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Activate a webhooks config by ID, to start receiving webhooks to its endpoint.
     * Activate one config
     * @param id Config ID
     */
    public activateConfig(id: string, _options?: Configuration): Promise<ConfigResponse> {
        const result = this.api.activateConfig(id, _options);
        return result.toPromise();
    }

    /**
     * Change the signing secret of the endpoint of a webhooks config.  If not passed or empty, a secret is automatically generated. The format is a random string of bytes of size 24, base64 encoded. (larger size after encoding) 
     * Change the signing secret of a config
     * @param id Config ID
     * @param configChangeSecret 
     */
    public changeConfigSecret(id: string, configChangeSecret?: ConfigChangeSecret, _options?: Configuration): Promise<ConfigResponse> {
        const result = this.api.changeConfigSecret(id, configChangeSecret, _options);
        return result.toPromise();
    }

    /**
     * Deactivate a webhooks config by ID, to stop receiving webhooks to its endpoint.
     * Deactivate one config
     * @param id Config ID
     */
    public deactivateConfig(id: string, _options?: Configuration): Promise<ConfigResponse> {
        const result = this.api.deactivateConfig(id, _options);
        return result.toPromise();
    }

    /**
     * Delete a webhooks config by ID.
     * Delete one config
     * @param id Config ID
     */
    public deleteConfig(id: string, _options?: Configuration): Promise<void> {
        const result = this.api.deleteConfig(id, _options);
        return result.toPromise();
    }

    /**
     * Sorted by updated date descending
     * Get many configs
     * @param id Optional filter by Config ID
     * @param endpoint Optional filter by endpoint URL
     */
    public getManyConfigs(id?: string, endpoint?: string, _options?: Configuration): Promise<ConfigsResponse> {
        const result = this.api.getManyConfigs(id, endpoint, _options);
        return result.toPromise();
    }

    /**
     * Insert a new webhooks config.  The endpoint should be a valid https URL and be unique.  The secret is the endpoint's verification secret. If not passed or empty, a secret is automatically generated. The format is a random string of bytes of size 24, base64 encoded. (larger size after encoding)  All eventTypes are converted to lower-case when inserted. 
     * Insert a new config
     * @param configUser 
     */
    public insertConfig(configUser: ConfigUser, _options?: Configuration): Promise<ConfigResponse> {
        const result = this.api.insertConfig(configUser, _options);
        return result.toPromise();
    }

    /**
     * Test a config by sending a webhook to its endpoint.
     * Test one config
     * @param id Config ID
     */
    public testConfig(id: string, _options?: Configuration): Promise<AttemptResponse> {
        const result = this.api.testConfig(id, _options);
        return result.toPromise();
    }


}



