# TransactionsResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Data** | [**[]Transaction**](Transaction.md) |  | 

## Methods

### NewTransactionsResponse

`func NewTransactionsResponse(data []Transaction, ) *TransactionsResponse`

NewTransactionsResponse instantiates a new TransactionsResponse object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewTransactionsResponseWithDefaults

`func NewTransactionsResponseWithDefaults() *TransactionsResponse`

NewTransactionsResponseWithDefaults instantiates a new TransactionsResponse object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetData

`func (o *TransactionsResponse) GetData() []Transaction`

GetData returns the Data field if non-nil, zero value otherwise.

### GetDataOk

`func (o *TransactionsResponse) GetDataOk() (*[]Transaction, bool)`

GetDataOk returns a tuple with the Data field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetData

`func (o *TransactionsResponse) SetData(v []Transaction)`

SetData sets Data field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


