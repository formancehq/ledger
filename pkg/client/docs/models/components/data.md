# Data


## Fields

| Field                                                            | Type                                                             | Required                                                         | Description                                                      |
| ---------------------------------------------------------------- | ---------------------------------------------------------------- | ---------------------------------------------------------------- | ---------------------------------------------------------------- |
| `State`                                                          | [components.State](../../models/components/state.md)             | :heavy_check_mark:                                               | Current state of the local node                                  |
| `Leader`                                                         | **int64*                                                         | :heavy_minus_sign:                                               | ID of the current leader (0 if no leader)                        |
| `LocalNode`                                                      | *int64*                                                          | :heavy_check_mark:                                               | ID of the local node                                             |
| `Nodes`                                                          | [][components.NodeInfo](../../models/components/nodeinfo.md)     | :heavy_minus_sign:                                               | List of all nodes in the cluster                                 |
| `InnerState`                                                     | [components.LedgerState](../../models/components/ledgerstate.md) | :heavy_check_mark:                                               | N/A                                                              |