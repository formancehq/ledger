package internal

const (
	BankingCircleConnector = "bankingcircle"
	CurrencyCloudConnector = "currencycloud"
	ModulrConnector        = "modulr"
	StripeConnector        = "stripe"
	WiseConnector          = "wise"
)

var AllConnectors = []string{
	BankingCircleConnector,
	CurrencyCloudConnector,
	ModulrConnector,
	StripeConnector,
	WiseConnector,
}
