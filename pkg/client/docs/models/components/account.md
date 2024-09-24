# Account


## Fields

| Field                                                    | Type                                                     | Required                                                 | Description                                              | Example                                                  |
| -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `Address`                                                | *string*                                                 | :heavy_check_mark:                                       | N/A                                                      | users:001                                                |
| `Type`                                                   | **string*                                                | :heavy_minus_sign:                                       | N/A                                                      | virtual                                                  |
| `Metadata`                                               | map[string]*any*                                         | :heavy_minus_sign:                                       | N/A                                                      | {<br/>"admin": true,<br/>"a": {<br/>"nested": {<br/>"key": "value"<br/>}<br/>}<br/>} |