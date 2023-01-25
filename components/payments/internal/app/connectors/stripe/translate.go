package stripe

import (
	"encoding/json"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/formancehq/payments/internal/app/ingestion"
	"github.com/formancehq/payments/internal/app/models"
	"github.com/stripe/stripe-go/v72"
)

type currency struct {
	decimals int
}

func currencies() map[string]currency {
	return map[string]currency{
		"ARS": {2}, //  Argentine Peso
		"AMD": {2}, //  Armenian Dram
		"AWG": {2}, //  Aruban Guilder
		"AUD": {2}, //  Australian Dollar
		"BSD": {2}, //  Bahamian Dollar
		"BHD": {3}, //  Bahraini Dinar
		"BDT": {2}, //  Bangladesh, Taka
		"BZD": {2}, //  Belize Dollar
		"BMD": {2}, //  Bermudian Dollar
		"BOB": {2}, //  Bolivia, Boliviano
		"BAM": {2}, //  Bosnia and Herzegovina, Convertible Marks
		"BWP": {2}, //  Botswana, Pula
		"BRL": {2}, //  Brazilian Real
		"BND": {2}, //  Brunei Dollar
		"CAD": {2}, //  Canadian Dollar
		"KYD": {2}, //  Cayman Islands Dollar
		"CLP": {0}, //  Chilean Peso
		"CNY": {2}, //  China Yuan Renminbi
		"COP": {2}, //  Colombian Peso
		"CRC": {2}, //  Costa Rican Colon
		"HRK": {2}, //  Croatian Kuna
		"CUC": {2}, //  Cuban Convertible Peso
		"CUP": {2}, //  Cuban Peso
		"CYP": {2}, //  Cyprus Pound
		"CZK": {2}, //  Czech Koruna
		"DKK": {2}, //  Danish Krone
		"DOP": {2}, //  Dominican Peso
		"XCD": {2}, //  East Caribbean Dollar
		"EGP": {2}, //  Egyptian Pound
		"SVC": {2}, //  El Salvador Colon
		"ATS": {2}, //  Euro
		"BEF": {2}, //  Euro
		"DEM": {2}, //  Euro
		"EEK": {2}, //  Euro
		"ESP": {2}, //  Euro
		"EUR": {2}, //  Euro
		"FIM": {2}, //  Euro
		"FRF": {2}, //  Euro
		"GRD": {2}, //  Euro
		"IEP": {2}, //  Euro
		"ITL": {2}, //  Euro
		"LUF": {2}, //  Euro
		"NLG": {2}, //  Euro
		"PTE": {2}, //  Euro
		"GHC": {2}, //  Ghana, Cedi
		"GIP": {2}, //  Gibraltar Pound
		"GTQ": {2}, //  Guatemala, Quetzal
		"HNL": {2}, //  Honduras, Lempira
		"HKD": {2}, //  Hong Kong Dollar
		"HUF": {0}, //  Hungary, Forint
		"ISK": {0}, //  Iceland Krona
		"INR": {2}, //  Indian Rupee
		"IDR": {2}, //  Indonesia, Rupiah
		"IRR": {2}, //  Iranian Rial
		"JMD": {2}, //  Jamaican Dollar
		"JPY": {0}, //  Japan, Yen
		"JOD": {3}, //  Jordanian Dinar
		"KES": {2}, //  Kenyan Shilling
		"KWD": {3}, //  Kuwaiti Dinar
		"LVL": {2}, //  Latvian Lats
		"LBP": {0}, //  Lebanese Pound
		"LTL": {2}, //  Lithuanian Litas
		"MKD": {2}, //  Macedonia, Denar
		"MYR": {2}, //  Malaysian Ringgit
		"MTL": {2}, //  Maltese Lira
		"MUR": {0}, //  Mauritius Rupee
		"MXN": {2}, //  Mexican Peso
		"MZM": {2}, //  Mozambique Metical
		"NPR": {2}, //  Nepalese Rupee
		"ANG": {2}, //  Netherlands Antillian Guilder
		"ILS": {2}, //  New Israeli Shekel
		"TRY": {2}, //  New Turkish Lira
		"NZD": {2}, //  New Zealand Dollar
		"NOK": {2}, //  Norwegian Krone
		"PKR": {2}, //  Pakistan Rupee
		"PEN": {2}, //  Peru, Nuevo Sol
		"UYU": {2}, //  Peso Uruguayo
		"PHP": {2}, //  Philippine Peso
		"PLN": {2}, //  Poland, Zloty
		"GBP": {2}, //  Pound Sterling
		"OMR": {3}, //  Rial Omani
		"RON": {2}, //  Romania, New Leu
		"ROL": {2}, //  Romania, Old Leu
		"RUB": {2}, //  Russian Ruble
		"SAR": {2}, //  Saudi Riyal
		"SGD": {2}, //  Singapore Dollar
		"SKK": {2}, //  Slovak Koruna
		"SIT": {2}, //  Slovenia, Tolar
		"ZAR": {2}, //  South Africa, Rand
		"KRW": {0}, //  South Korea, Won
		"SZL": {2}, //  Swaziland, Lilangeni
		"SEK": {2}, //  Swedish Krona
		"CHF": {2}, //  Swiss Franc
		"TZS": {2}, //  Tanzanian Shilling
		"THB": {2}, //  Thailand, Baht
		"TOP": {2}, //  Tonga, Paanga
		"AED": {2}, //  UAE Dirham
		"UAH": {2}, //  Ukraine, Hryvnia
		"USD": {2}, //  US Dollar
		"VUV": {0}, //  Vanuatu, Vatu
		"VEF": {2}, //  Venezuela Bolivares Fuertes
		"VEB": {2}, //  Venezuela, Bolivar
		"VND": {0}, //  Viet Nam, Dong
		"ZWD": {2}, //  Zimbabwe Dollar
	}
}

func CreateBatchElement(balanceTransaction *stripe.BalanceTransaction, forward bool) (ingestion.PaymentBatchElement, bool) {
	var payment models.Payment // reference   payments.Referenced
	// paymentData *payments.Data
	// adjustment  *payments.Adjustment

	defer func() {
		// DEBUG
		if e := recover(); e != nil {
			log.Println("Error translating transaction")
			debug.PrintStack()
			spew.Dump(balanceTransaction)
			panic(e)
		}
	}()

	if balanceTransaction.Source == nil {
		return ingestion.PaymentBatchElement{}, false
	}

	if balanceTransaction.Source.Payout == nil && balanceTransaction.Source.Charge == nil {
		return ingestion.PaymentBatchElement{}, false
	}

	formatAsset := func(cur stripe.Currency) models.PaymentAsset {
		asset := strings.ToUpper(string(cur))

		def, ok := currencies()[asset]
		if !ok {
			return models.PaymentAsset(asset)
		}

		if def.decimals == 0 {
			return models.PaymentAsset(asset)
		}

		return models.PaymentAsset(fmt.Sprintf("%s/%d", asset, def.decimals))
	}

	rawData, err := json.Marshal(balanceTransaction)
	if err != nil {
		return ingestion.PaymentBatchElement{}, false
	}

	switch balanceTransaction.Type {
	case stripe.BalanceTransactionTypeCharge:
		payment = models.Payment{
			Reference: balanceTransaction.Source.Charge.ID,
			Type:      models.PaymentTypePayIn,
			Status:    models.PaymentStatusSucceeded,
			Amount:    balanceTransaction.Source.Charge.Amount,
			Asset:     formatAsset(balanceTransaction.Source.Charge.Currency),
			RawData:   rawData,
			Scheme:    models.PaymentScheme(balanceTransaction.Source.Charge.PaymentMethodDetails.Card.Brand),
			CreatedAt: time.Unix(balanceTransaction.Created, 0),
		}
	case stripe.BalanceTransactionTypePayout:
		payment = models.Payment{
			Reference: balanceTransaction.Source.Payout.ID,
			Type:      models.PaymentTypePayOut,
			Status:    convertPayoutStatus(balanceTransaction.Source.Payout.Status),
			Amount:    balanceTransaction.Source.Payout.Amount,
			RawData:   rawData,
			Asset:     formatAsset(balanceTransaction.Source.Payout.Currency),
			Scheme: func() models.PaymentScheme {
				switch balanceTransaction.Source.Payout.Type {
				case stripe.PayoutTypeBank:
					return models.PaymentSchemeSepaCredit
				case stripe.PayoutTypeCard:
					return models.PaymentScheme(balanceTransaction.Source.Payout.Card.Brand)
				}

				return models.PaymentSchemeUnknown
			}(),
			CreatedAt: time.Unix(balanceTransaction.Created, 0),
		}
	case stripe.BalanceTransactionTypeTransfer:
		payment = models.Payment{
			Reference: balanceTransaction.Source.Transfer.ID,
			Type:      models.PaymentTypePayOut,
			Status:    models.PaymentStatusSucceeded,
			Amount:    balanceTransaction.Source.Transfer.Amount,
			RawData:   rawData,
			Asset:     formatAsset(balanceTransaction.Source.Transfer.Currency),
			Scheme:    models.PaymentSchemeOther,
			CreatedAt: time.Unix(balanceTransaction.Created, 0),
		}
	case stripe.BalanceTransactionTypeRefund:
		payment = models.Payment{
			Reference: balanceTransaction.Source.Refund.Charge.ID,
			Type:      models.PaymentTypePayOut,
			Adjustments: []*models.Adjustment{
				{
					Reference: balanceTransaction.Source.Refund.Charge.ID,
					Status:    models.PaymentStatusSucceeded,
					Amount:    balanceTransaction.Amount,
					CreatedAt: time.Unix(balanceTransaction.Created, 0),
					RawData:   rawData,
				},
			},
		}
	case stripe.BalanceTransactionTypePayment:
		payment = models.Payment{
			Reference: balanceTransaction.Source.Charge.ID,
			Type:      models.PaymentTypePayIn,
			Status:    models.PaymentStatusSucceeded,
			Amount:    balanceTransaction.Source.Charge.Amount,
			RawData:   rawData,
			Asset:     formatAsset(balanceTransaction.Source.Charge.Currency),
			Scheme:    models.PaymentSchemeOther,
			CreatedAt: time.Unix(balanceTransaction.Created, 0),
		}
	case stripe.BalanceTransactionTypePayoutCancel:
		payment = models.Payment{
			Reference: balanceTransaction.Source.Payout.ID,
			Type:      models.PaymentTypePayOut,
			Status:    models.PaymentStatusFailed,
			Adjustments: []*models.Adjustment{
				{
					Reference: balanceTransaction.Source.Payout.ID,
					Status:    convertPayoutStatus(balanceTransaction.Source.Payout.Status),
					CreatedAt: time.Unix(balanceTransaction.Created, 0),
					RawData:   rawData,
					Absolute:  true,
				},
			},
		}
	case stripe.BalanceTransactionTypePayoutFailure:
		payment = models.Payment{
			Reference: balanceTransaction.Source.Payout.ID,
			Type:      models.PaymentTypePayIn,
			Status:    models.PaymentStatusFailed,
			Adjustments: []*models.Adjustment{
				{
					Reference: balanceTransaction.Source.Payout.ID,
					Status:    convertPayoutStatus(balanceTransaction.Source.Payout.Status),
					CreatedAt: time.Unix(balanceTransaction.Created, 0),
					RawData:   rawData,
					Absolute:  true,
				},
			},
		}
	case stripe.BalanceTransactionTypePaymentRefund:
		payment = models.Payment{
			Reference: balanceTransaction.Source.Refund.Charge.ID,
			Type:      models.PaymentTypePayOut,
			Status:    models.PaymentStatusSucceeded,
			Adjustments: []*models.Adjustment{
				{
					Reference: balanceTransaction.Source.Refund.Charge.ID,
					Status:    models.PaymentStatusSucceeded,
					Amount:    balanceTransaction.Amount,
					CreatedAt: time.Unix(balanceTransaction.Created, 0),
					RawData:   rawData,
				},
			},
		}
	case stripe.BalanceTransactionTypeAdjustment:
		payment = models.Payment{
			Reference: balanceTransaction.Source.Dispute.Charge.ID,
			Type:      models.PaymentTypePayOut,
			Adjustments: []*models.Adjustment{
				{
					Reference: balanceTransaction.Source.Dispute.Charge.ID,
					Status:    models.PaymentStatusCancelled,
					Amount:    balanceTransaction.Amount,
					CreatedAt: time.Unix(balanceTransaction.Created, 0),
					RawData:   rawData,
				},
			},
		}
	case stripe.BalanceTransactionTypeStripeFee:
		return ingestion.PaymentBatchElement{}, false
	default:
		return ingestion.PaymentBatchElement{}, false
	}

	return ingestion.PaymentBatchElement{
		Payment: &payment,
		Update:  forward,
	}, true
}

func convertPayoutStatus(status stripe.PayoutStatus) models.PaymentStatus {
	switch status {
	case stripe.PayoutStatusCanceled:
		return models.PaymentStatusCancelled
	case stripe.PayoutStatusFailed:
		return models.PaymentStatusFailed
	case stripe.PayoutStatusInTransit, stripe.PayoutStatusPending:
		return models.PaymentStatusPending
	case stripe.PayoutStatusPaid:
		return models.PaymentStatusSucceeded
	}

	return models.PaymentStatusOther
}
