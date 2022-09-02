# LedgerStorage

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Driver** | **string** |  | 
**Ledgers** | **[]string** |  | 

## Methods

### NewLedgerStorage

`func NewLedgerStorage(driver string, ledgers []string, ) *LedgerStorage`

NewLedgerStorage instantiates a new LedgerStorage object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewLedgerStorageWithDefaults

`func NewLedgerStorageWithDefaults() *LedgerStorage`

NewLedgerStorageWithDefaults instantiates a new LedgerStorage object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetDriver

`func (o *LedgerStorage) GetDriver() string`

GetDriver returns the Driver field if non-nil, zero value otherwise.

### GetDriverOk

`func (o *LedgerStorage) GetDriverOk() (*string, bool)`

GetDriverOk returns a tuple with the Driver field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDriver

`func (o *LedgerStorage) SetDriver(v string)`

SetDriver sets Driver field to given value.


### GetLedgers

`func (o *LedgerStorage) GetLedgers() []string`

GetLedgers returns the Ledgers field if non-nil, zero value otherwise.

### GetLedgersOk

`func (o *LedgerStorage) GetLedgersOk() (*[]string, bool)`

GetLedgersOk returns a tuple with the Ledgers field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLedgers

`func (o *LedgerStorage) SetLedgers(v []string)`

SetLedgers sets Ledgers field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


