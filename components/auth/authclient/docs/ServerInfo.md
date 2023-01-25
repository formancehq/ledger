# ServerInfo

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Version** | **interface{}** |  |

## Methods

### NewServerInfo

`func NewServerInfo(version interface{}, ) *ServerInfo`

NewServerInfo instantiates a new ServerInfo object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewServerInfoWithDefaults

`func NewServerInfoWithDefaults() *ServerInfo`

NewServerInfoWithDefaults instantiates a new ServerInfo object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetVersion

`func (o *ServerInfo) GetVersion() interface{}`

GetVersion returns the Version field if non-nil, zero value otherwise.

### GetVersionOk

`func (o *ServerInfo) GetVersionOk() (*interface{}, bool)`

GetVersionOk returns a tuple with the Version field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetVersion

`func (o *ServerInfo) SetVersion(v interface{})`

SetVersion sets Version field to given value.


### SetVersionNil

`func (o *ServerInfo) SetVersionNil(b bool)`

 SetVersionNil sets the value for Version to be an explicit nil

### UnsetVersion
`func (o *ServerInfo) UnsetVersion()`

UnsetVersion ensures that no value is present for Version, not even an explicit nil

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
