# NodeInfo


## Fields

| Field                                                      | Type                                                       | Required                                                   | Description                                                |
| ---------------------------------------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------- |
| `ID`                                                       | *int64*                                                    | :heavy_check_mark:                                         | Node ID                                                    |
| `Address`                                                  | *string*                                                   | :heavy_check_mark:                                         | Node address                                               |
| `Suffrage`                                                 | [components.Suffrage](../../models/components/suffrage.md) | :heavy_check_mark:                                         | Node suffrage (Voter, Nonvoter, or Learner)                |