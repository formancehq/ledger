# ClusterStateResponseData


## Fields

| Field                                                        | Type                                                         | Required                                                     | Description                                                  |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `State`                                                      | [*components.State](../../models/components/state.md)        | :heavy_minus_sign:                                           | Current state of the local node                              |
| `Leader`                                                     | **string*                                                    | :heavy_minus_sign:                                           | ID of the current leader (empty if no leader)                |
| `LocalNode`                                                  | **string*                                                    | :heavy_minus_sign:                                           | ID of the local node                                         |
| `Nodes`                                                      | [][components.NodeInfo](../../models/components/nodeinfo.md) | :heavy_minus_sign:                                           | List of all nodes in the cluster                             |