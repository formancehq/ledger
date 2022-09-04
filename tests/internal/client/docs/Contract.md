# Contract

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Account** | Pointer to **string** |  | [optional] 
**Expr** | **map[string]interface{}** |  | 

## Methods

### NewContract

`func NewContract(expr map[string]interface{}, ) *Contract`

NewContract instantiates a new Contract object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewContractWithDefaults

`func NewContractWithDefaults() *Contract`

NewContractWithDefaults instantiates a new Contract object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetAccount

`func (o *Contract) GetAccount() string`

GetAccount returns the Account field if non-nil, zero value otherwise.

### GetAccountOk

`func (o *Contract) GetAccountOk() (*string, bool)`

GetAccountOk returns a tuple with the Account field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAccount

`func (o *Contract) SetAccount(v string)`

SetAccount sets Account field to given value.

### HasAccount

`func (o *Contract) HasAccount() bool`

HasAccount returns a boolean if a field has been set.

### GetExpr

`func (o *Contract) GetExpr() map[string]interface{}`

GetExpr returns the Expr field if non-nil, zero value otherwise.

### GetExprOk

`func (o *Contract) GetExprOk() (*map[string]interface{}, bool)`

GetExprOk returns a tuple with the Expr field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetExpr

`func (o *Contract) SetExpr(v map[string]interface{})`

SetExpr sets Expr field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


