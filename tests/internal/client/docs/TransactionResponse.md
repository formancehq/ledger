# TransactionResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Data** | [**Transaction**](Transaction.md) |  | 

## Methods

### NewTransactionResponse

`func NewTransactionResponse(data Transaction, ) *TransactionResponse`

NewTransactionResponse instantiates a new TransactionResponse object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewTransactionResponseWithDefaults

`func NewTransactionResponseWithDefaults() *TransactionResponse`

NewTransactionResponseWithDefaults instantiates a new TransactionResponse object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetData

`func (o *TransactionResponse) GetData() Transaction`

GetData returns the Data field if non-nil, zero value otherwise.

### GetDataOk

`func (o *TransactionResponse) GetDataOk() (*Transaction, bool)`

GetDataOk returns a tuple with the Data field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetData

`func (o *TransactionResponse) SetData(v Transaction)`

SetData sets Data field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


