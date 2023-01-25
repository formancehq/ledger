# ListConnectorsResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Data** | [**[]ConnectorBaseInfo**](ConnectorBaseInfo.md) |  |

## Methods

### NewListConnectorsResponse

`func NewListConnectorsResponse(data []ConnectorBaseInfo, ) *ListConnectorsResponse`

NewListConnectorsResponse instantiates a new ListConnectorsResponse object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewListConnectorsResponseWithDefaults

`func NewListConnectorsResponseWithDefaults() *ListConnectorsResponse`

NewListConnectorsResponseWithDefaults instantiates a new ListConnectorsResponse object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetData

`func (o *ListConnectorsResponse) GetData() []ConnectorBaseInfo`

GetData returns the Data field if non-nil, zero value otherwise.

### GetDataOk

`func (o *ListConnectorsResponse) GetDataOk() (*[]ConnectorBaseInfo, bool)`

GetDataOk returns a tuple with the Data field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetData

`func (o *ListConnectorsResponse) SetData(v []ConnectorBaseInfo)`

SetData sets Data field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
