package actions

// GRPCRetryPolicy retries UNAVAILABLE through one leader election: five total
// attempts with four exponential backoffs totaling 3.75s before jitter.
// Five is grpc-go's default maximum number of attempts per call.
var GRPCRetryPolicy = `{
	"methodConfig": [{
		"name": [{}],
		"retryPolicy": {
			"MaxAttempts": 5,
			"InitialBackoff": "0.25s",
			"MaxBackoff": "2s",
			"BackoffMultiplier": 2.0,
			"RetryableStatusCodes": ["UNAVAILABLE"]
		}
	}]
}`
