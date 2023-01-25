package fctl

import (
	"io"

	"github.com/pterm/pterm"
)

func Println(args ...any) {
	pterm.Println(args...)
}

func TextWriter(out io.Writer) *pterm.BasicTextPrinter {
	return pterm.DefaultBasicText.WithWriter(out)
}

func Highlightln(out io.Writer, format string, args ...any) {
	TextWriter(out).WithStyle(pterm.NewStyle(pterm.FgLightCyan)).Printfln(format, args...)
}

func SuccessWriter(out io.Writer) *pterm.PrefixPrinter {
	return pterm.Success.WithWriter(out)
}

func Success(out io.Writer, format string, args ...any) {
	SuccessWriter(out).Printfln(format, args...)
}

func ErrorWriter(out io.Writer) *pterm.PrefixPrinter {
	return pterm.Error.WithWriter(out)
}

func Error(out io.Writer, format string, args ...any) {
	ErrorWriter(out).Printfln(format, args...)
}

var Section = pterm.SectionPrinter{
	Style:           &pterm.ThemeDefault.SectionStyle,
	Level:           1,
	TopPadding:      0,
	BottomPadding:   0,
	IndentCharacter: "#",
}
