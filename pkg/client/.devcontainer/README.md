
> **Remember to shutdown a GitHub Codespace when it is not in use!**

# Dev Containers Quick Start

The default location for usage snippets is the `samples` directory.

## Running a Usage Sample

A sample usage example has been provided in a `root.go` file. As you work with the SDK, it's expected that you will modify these samples to fit your needs. To execute this particular snippet, use the command below.

```
go run root.go
```

## Generating Additional Usage Samples

The speakeasy CLI allows you to generate more usage snippets. Here's how:

- To generate a sample for a specific operation by providing an operation ID, use:

```
speakeasy generate usage -s openapi/v2.yaml -l go -i {INPUT_OPERATION_ID} -o ./samples
```

- To generate samples for an entire namespace (like a tag or group name), use:

```
speakeasy generate usage -s openapi/v2.yaml -l go -n {INPUT_TAG_NAME} -o ./samples
```
