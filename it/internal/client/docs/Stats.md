# Stats

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Accounts** | **int32** |  | 
**Transactions** | **int32** |  | 

## Methods

### NewStats

`func NewStats(accounts int32, transactions int32, ) *Stats`

NewStats instantiates a new Stats object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewStatsWithDefaults

`func NewStatsWithDefaults() *Stats`

NewStatsWithDefaults instantiates a new Stats object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetAccounts

`func (o *Stats) GetAccounts() int32`

GetAccounts returns the Accounts field if non-nil, zero value otherwise.

### GetAccountsOk

`func (o *Stats) GetAccountsOk() (*int32, bool)`

GetAccountsOk returns a tuple with the Accounts field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAccounts

`func (o *Stats) SetAccounts(v int32)`

SetAccounts sets Accounts field to given value.


### GetTransactions

`func (o *Stats) GetTransactions() int32`

GetTransactions returns the Transactions field if non-nil, zero value otherwise.

### GetTransactionsOk

`func (o *Stats) GetTransactionsOk() (*int32, bool)`

GetTransactionsOk returns a tuple with the Transactions field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTransactions

`func (o *Stats) SetTransactions(v int32)`

SetTransactions sets Transactions field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


