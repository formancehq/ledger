# Wallet

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | **string** | The unique ID of the wallet. | 
**Metadata** | **map[string]interface{}** | Metadata associated with the wallet. | 
**Name** | **string** |  | 
**CreatedAt** | **time.Time** |  | 
**Ledger** | **string** |  | 

## Methods

### NewWallet

`func NewWallet(id string, metadata map[string]interface{}, name string, createdAt time.Time, ledger string, ) *Wallet`

NewWallet instantiates a new Wallet object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewWalletWithDefaults

`func NewWalletWithDefaults() *Wallet`

NewWalletWithDefaults instantiates a new Wallet object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *Wallet) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *Wallet) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *Wallet) SetId(v string)`

SetId sets Id field to given value.


### GetMetadata

`func (o *Wallet) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *Wallet) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *Wallet) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.


### GetName

`func (o *Wallet) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *Wallet) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *Wallet) SetName(v string)`

SetName sets Name field to given value.


### GetCreatedAt

`func (o *Wallet) GetCreatedAt() time.Time`

GetCreatedAt returns the CreatedAt field if non-nil, zero value otherwise.

### GetCreatedAtOk

`func (o *Wallet) GetCreatedAtOk() (*time.Time, bool)`

GetCreatedAtOk returns a tuple with the CreatedAt field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCreatedAt

`func (o *Wallet) SetCreatedAt(v time.Time)`

SetCreatedAt sets CreatedAt field to given value.


### GetLedger

`func (o *Wallet) GetLedger() string`

GetLedger returns the Ledger field if non-nil, zero value otherwise.

### GetLedgerOk

`func (o *Wallet) GetLedgerOk() (*string, bool)`

GetLedgerOk returns a tuple with the Ledger field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLedger

`func (o *Wallet) SetLedger(v string)`

SetLedger sets Ledger field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


