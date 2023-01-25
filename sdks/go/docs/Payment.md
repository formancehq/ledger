# Payment

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | **string** |  | 
**Reference** | **string** |  | 
**AccountID** | **string** |  | 
**Type** | **string** |  | 
**Provider** | [**Connector**](Connector.md) |  | 
**Status** | [**PaymentStatus**](PaymentStatus.md) |  | 
**InitialAmount** | **int64** |  | 
**Scheme** | **string** |  | 
**Asset** | **string** |  | 
**CreatedAt** | **time.Time** |  | 
**Raw** | **map[string]interface{}** |  | 
**Adjustments** | [**[]PaymentAdjustment**](PaymentAdjustment.md) |  | 
**Metadata** | [**[]PaymentMetadata**](PaymentMetadata.md) |  | 

## Methods

### NewPayment

`func NewPayment(id string, reference string, accountID string, type_ string, provider Connector, status PaymentStatus, initialAmount int64, scheme string, asset string, createdAt time.Time, raw map[string]interface{}, adjustments []PaymentAdjustment, metadata []PaymentMetadata, ) *Payment`

NewPayment instantiates a new Payment object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewPaymentWithDefaults

`func NewPaymentWithDefaults() *Payment`

NewPaymentWithDefaults instantiates a new Payment object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *Payment) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *Payment) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *Payment) SetId(v string)`

SetId sets Id field to given value.


### GetReference

`func (o *Payment) GetReference() string`

GetReference returns the Reference field if non-nil, zero value otherwise.

### GetReferenceOk

`func (o *Payment) GetReferenceOk() (*string, bool)`

GetReferenceOk returns a tuple with the Reference field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReference

`func (o *Payment) SetReference(v string)`

SetReference sets Reference field to given value.


### GetAccountID

`func (o *Payment) GetAccountID() string`

GetAccountID returns the AccountID field if non-nil, zero value otherwise.

### GetAccountIDOk

`func (o *Payment) GetAccountIDOk() (*string, bool)`

GetAccountIDOk returns a tuple with the AccountID field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAccountID

`func (o *Payment) SetAccountID(v string)`

SetAccountID sets AccountID field to given value.


### GetType

`func (o *Payment) GetType() string`

GetType returns the Type field if non-nil, zero value otherwise.

### GetTypeOk

`func (o *Payment) GetTypeOk() (*string, bool)`

GetTypeOk returns a tuple with the Type field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetType

`func (o *Payment) SetType(v string)`

SetType sets Type field to given value.


### GetProvider

`func (o *Payment) GetProvider() Connector`

GetProvider returns the Provider field if non-nil, zero value otherwise.

### GetProviderOk

`func (o *Payment) GetProviderOk() (*Connector, bool)`

GetProviderOk returns a tuple with the Provider field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetProvider

`func (o *Payment) SetProvider(v Connector)`

SetProvider sets Provider field to given value.


### GetStatus

`func (o *Payment) GetStatus() PaymentStatus`

GetStatus returns the Status field if non-nil, zero value otherwise.

### GetStatusOk

`func (o *Payment) GetStatusOk() (*PaymentStatus, bool)`

GetStatusOk returns a tuple with the Status field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetStatus

`func (o *Payment) SetStatus(v PaymentStatus)`

SetStatus sets Status field to given value.


### GetInitialAmount

`func (o *Payment) GetInitialAmount() int64`

GetInitialAmount returns the InitialAmount field if non-nil, zero value otherwise.

### GetInitialAmountOk

`func (o *Payment) GetInitialAmountOk() (*int64, bool)`

GetInitialAmountOk returns a tuple with the InitialAmount field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetInitialAmount

`func (o *Payment) SetInitialAmount(v int64)`

SetInitialAmount sets InitialAmount field to given value.


### GetScheme

`func (o *Payment) GetScheme() string`

GetScheme returns the Scheme field if non-nil, zero value otherwise.

### GetSchemeOk

`func (o *Payment) GetSchemeOk() (*string, bool)`

GetSchemeOk returns a tuple with the Scheme field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetScheme

`func (o *Payment) SetScheme(v string)`

SetScheme sets Scheme field to given value.


### GetAsset

`func (o *Payment) GetAsset() string`

GetAsset returns the Asset field if non-nil, zero value otherwise.

### GetAssetOk

`func (o *Payment) GetAssetOk() (*string, bool)`

GetAssetOk returns a tuple with the Asset field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAsset

`func (o *Payment) SetAsset(v string)`

SetAsset sets Asset field to given value.


### GetCreatedAt

`func (o *Payment) GetCreatedAt() time.Time`

GetCreatedAt returns the CreatedAt field if non-nil, zero value otherwise.

### GetCreatedAtOk

`func (o *Payment) GetCreatedAtOk() (*time.Time, bool)`

GetCreatedAtOk returns a tuple with the CreatedAt field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCreatedAt

`func (o *Payment) SetCreatedAt(v time.Time)`

SetCreatedAt sets CreatedAt field to given value.


### GetRaw

`func (o *Payment) GetRaw() map[string]interface{}`

GetRaw returns the Raw field if non-nil, zero value otherwise.

### GetRawOk

`func (o *Payment) GetRawOk() (*map[string]interface{}, bool)`

GetRawOk returns a tuple with the Raw field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRaw

`func (o *Payment) SetRaw(v map[string]interface{})`

SetRaw sets Raw field to given value.


### GetAdjustments

`func (o *Payment) GetAdjustments() []PaymentAdjustment`

GetAdjustments returns the Adjustments field if non-nil, zero value otherwise.

### GetAdjustmentsOk

`func (o *Payment) GetAdjustmentsOk() (*[]PaymentAdjustment, bool)`

GetAdjustmentsOk returns a tuple with the Adjustments field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAdjustments

`func (o *Payment) SetAdjustments(v []PaymentAdjustment)`

SetAdjustments sets Adjustments field to given value.


### GetMetadata

`func (o *Payment) GetMetadata() []PaymentMetadata`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *Payment) GetMetadataOk() (*[]PaymentMetadata, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *Payment) SetMetadata(v []PaymentMetadata)`

SetMetadata sets Metadata field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


