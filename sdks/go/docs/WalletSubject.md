# WalletSubject

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Type** | **string** |  | 
**Identifier** | **string** |  | 
**Balance** | Pointer to **string** |  | [optional] 

## Methods

### NewWalletSubject

`func NewWalletSubject(type_ string, identifier string, ) *WalletSubject`

NewWalletSubject instantiates a new WalletSubject object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewWalletSubjectWithDefaults

`func NewWalletSubjectWithDefaults() *WalletSubject`

NewWalletSubjectWithDefaults instantiates a new WalletSubject object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetType

`func (o *WalletSubject) GetType() string`

GetType returns the Type field if non-nil, zero value otherwise.

### GetTypeOk

`func (o *WalletSubject) GetTypeOk() (*string, bool)`

GetTypeOk returns a tuple with the Type field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetType

`func (o *WalletSubject) SetType(v string)`

SetType sets Type field to given value.


### GetIdentifier

`func (o *WalletSubject) GetIdentifier() string`

GetIdentifier returns the Identifier field if non-nil, zero value otherwise.

### GetIdentifierOk

`func (o *WalletSubject) GetIdentifierOk() (*string, bool)`

GetIdentifierOk returns a tuple with the Identifier field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetIdentifier

`func (o *WalletSubject) SetIdentifier(v string)`

SetIdentifier sets Identifier field to given value.


### GetBalance

`func (o *WalletSubject) GetBalance() string`

GetBalance returns the Balance field if non-nil, zero value otherwise.

### GetBalanceOk

`func (o *WalletSubject) GetBalanceOk() (*string, bool)`

GetBalanceOk returns a tuple with the Balance field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetBalance

`func (o *WalletSubject) SetBalance(v string)`

SetBalance sets Balance field to given value.

### HasBalance

`func (o *WalletSubject) HasBalance() bool`

HasBalance returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


