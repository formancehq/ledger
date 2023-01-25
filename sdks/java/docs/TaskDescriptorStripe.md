

# TaskDescriptorStripe


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**provider** | **String** | The connector code |  [optional] |
|**createdAt** | **OffsetDateTime** | The date when the task was created |  [optional] |
|**status** | [**StatusEnum**](#StatusEnum) | The task status |  [optional] |
|**error** | **String** | The error message if the task failed |  [optional] |
|**state** | **Object** | The task state |  [optional] |
|**descriptor** | [**TaskDescriptorStripeDescriptor**](TaskDescriptorStripeDescriptor.md) |  |  [optional] |



## Enum: StatusEnum

| Name | Value |
|---- | -----|
| STOPPED | &quot;stopped&quot; |
| PENDING | &quot;pending&quot; |
| TERMINATED | &quot;terminated&quot; |
| ACTIVE | &quot;active&quot; |
| FAILED | &quot;failed&quot; |



