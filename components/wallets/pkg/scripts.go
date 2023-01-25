//nolint:golint
package wallet

import (
	"bytes"
	_ "embed"
	"text/template"
)

var (
	//go:embed numscript/confirm-hold.num
	ConfirmHoldScript string
	//go:embed numscript/cancel-hold.num
	CancelHoldScript string
	//go:embed numscript/credit-wallet.num
	CreditWalletScript string
	//go:embed numscript/debit-wallet.num
	DebitWalletScript string
)

func renderTemplate(tplStr string, data any) string {
	buf := bytes.NewBufferString("")
	tpl := template.Must(template.New("tpl").Parse(tplStr))
	if err := tpl.Execute(buf, data); err != nil {
		panic(err)
	}
	return buf.String()
}

func BuildConfirmHoldScript(final bool, asset string) string {
	return renderTemplate(ConfirmHoldScript, map[string]any{
		"Final": final,
		"Asset": asset,
	})
}

func BuildCreditWalletScript(sources ...string) string {
	return renderTemplate(CreditWalletScript, map[string]any{
		"Sources": sources,
	})
}

func BuildDebitWalletScript(sources ...string) string {
	return renderTemplate(DebitWalletScript, map[string]any{
		"Sources": sources,
	})
}

func BuildCancelHoldScript(asset string) string {
	return renderTemplate(CancelHoldScript, map[string]any{
		"Asset": asset,
	})
}
