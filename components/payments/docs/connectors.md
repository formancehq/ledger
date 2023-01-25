# Connectors

## Currently supported connectors configs and typical default values

- [BankingCircle](#bankingcircle)
- [CurrencyCloud](#currencycloud)
- [Modulr](#modulr)
- [Stripe](#stripe)
- [Wise](#wise)
- [DummyPay](#dummypay)

### BankingCircle

Docs: [https://docs.bankingcircleconnect.com/](https://docs.bankingcircleconnect.com/)

```golang
    Username              string `json:"username" yaml:"username" bson:"username"`
    Password              string `json:"password" yaml:"password" bson:"password"`
    Endpoint              string `json:"endpoint" yaml:"endpoint" bson:"endpoint"`
    AuthorizationEndpoint string `json:"authorizationEndpoint" yaml:"authorizationEndpoint" bson:"authorizationEndpoint"`
```

#### Sandbox defaults
```json
{
    "username": "username",
    "password": "password",
    "endpoint": "https://sandbox.bankingcircle.com",
    "authorizationEndpoint": "https://authorizationsandbox.bankingcircleconnect.com"
}
```

#### Production defaults
```json
{
    "username": "username",
    "password": "password",
    "endpoint": " https://www.bankingcircleconnect.com/",
    "authorizationEndpoint": "https://authorization.bankingcircleconnect.com"
}
```

### CurrencyCloud

Docs: [https://www.currencycloud.com/developers/](https://www.currencycloud.com/developers/)

```golang
	LoginID       string   `json:"loginID" bson:"loginID"`
	APIKey        string   `json:"apiKey" bson:"apiKey"`
	Endpoint      string   `json:"endpoint" bson:"endpoint"`
	PollingPeriod Duration `json:"pollingPeriod" bson:"pollingPeriod"`
```

#### Demo defaults
```json
{
    "loginID": "loginID",
    "apiKey": "apiKey",
    "endpoint": "https://devapi.currencycloud.com",
    "pollingPeriod": "1m"
}
```

#### Production defaults
```json
{
    "loginID": "loginID",
    "apiKey": "apiKey",
    "endpoint": "https://api.currencycloud.com",
    "pollingPeriod": "1m"
}
```

### Modulr

Docs: [https://www.modulrfinance.com/modulr-api](https://www.modulrfinance.com/modulr-api)

```golang
    APIKey    string `json:"apiKey" bson:"apiKey"`
    APISecret string `json:"apiSecret" bson:"apiSecret"`
    Endpoint  string `json:"endpoint" bson:"endpoint"`
```

#### Sandbox defaults
```json
{
    "apiKey": "apiKey",
    "apiSecret": "apiSecret",
    "endpoint": "https://api-sandbox.modulrfinance.com"
}
```

#### Production defaults
```json
{
    "apiKey": "apiKey",
    "apiSecret": "apiSecret",
    "endpoint": "https://api.modulrfinance.com"
}
```

### Stripe

Docs: [https://stripe.com/docs/development](https://stripe.com/docs/development)

Sandbox/Production environment selection is controlled by api key and api secret types.

```golang
    PollingPeriod  connectors.Duration `json:"pollingPeriod" yaml:"pollingPeriod" bson:"pollingPeriod"`
    APIKey         string              `json:"apiKey" yaml:"apiKey" bson:"apiKey"`
    PageSize       uint64              `json:"pageSize" yaml:"pageSize" bson:"pageSize"`
```

#### Defaults
```json
{
    "pollingPeriod": "1m",
    "apiKey": "apiKey",
    "pageSize": 100
}
```

### Wise

Docs: [https://api-docs.wise.com/](https://api-docs.wise.com/)

Sandbox/Production environment selection is controlled by api key and api secret types.

```golang
    APIKey    string `json:"apiKey
````

#### Defaults
```json
{
    "apiKey": "apiKey"
}
```

### DummyPay

This connector is used only for testing purposes. It does not connect to any real payment provider.

```golang
	Directory            string              `json:"directory" yaml:"directory" bson:"directory"`
	FilePollingPeriod    connectors.Duration `json:"filePollingPeriod" yaml:"filePollingPeriod" bson:"filePollingPeriod"`
	FileGenerationPeriod connectors.Duration `json:"fileGenerationPeriod" yaml:"fileGenerationPeriod" bson:"fileGenerationPeriod"`
```

#### Defaults
```json
{
  "directory": "/tmp/payments",
  "filePollingPeriod": "30s",
  "fileGenerationPeriod": "10s"
}
```
