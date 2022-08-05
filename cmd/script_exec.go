package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	previewFlag = "preview"
)

func NewScriptExec() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "exec [ledger] [script]",
		Args: cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			b, err := ioutil.ReadFile(args[1])
			if err != nil {
				logrus.Fatal(err)
			}

			r := regexp.MustCompile(`^\n`)
			s := string(b)
			s = r.ReplaceAllString(s, "")

			b, err = json.Marshal(gin.H{
				"plain": s,
			})
			if err != nil {
				logrus.Fatal(err)
			}

			logrus.Debugln(string(b))

			req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/%s/script",
				viper.Get(serverHttpBindAddressFlag),
				args[0]), bytes.NewReader(b))
			if err != nil {
				logrus.Fatal(err)
			}

			values := url.Values{}
			if viper.GetBool(previewFlag) {
				values.Set("preview", "yes")
			}
			req.URL.RawQuery = values.Encode()
			req.Header.Set("Content-Type", "application/json")

			res, err := http.DefaultClient.Do(req)
			if err != nil {
				logrus.Fatal(err)
			}

			result := controllers.ScriptResponse{}
			err = json.NewDecoder(res.Body).Decode(&result)
			if err != nil {
				logrus.Fatal(err)
			}

			if result.ErrorCode != "" {
				switch result.ErrorCode {
				case "INTERNAL":
					logrus.Fatal("unexpected error occured")
				default:
					logrus.Fatal(result.ErrorCode, result.ErrorMessage)
				}
			}

			fmt.Println("Script ran successfully ✅")
			fmt.Println("Tx resume:")
			fmt.Printf("ID: %d\r\n", result.Transaction.ID)
			fmt.Println("Postings:")
			for _, p := range result.Transaction.Postings {
				fmt.Printf(
					"\t Source: %s, Destination: %s, Amount: %s, Asset: %s\r\n",
					p.Source,
					p.Destination,
					p.Amount.String(),
					p.Asset,
				)
			}
			if !viper.GetBool(previewFlag) {
				fmt.Printf("Created transaction: http://%s/%s/transactions/%d\r\n",
					viper.Get(serverHttpBindAddressFlag),
					args[0],
					result.Transaction.ID)
			}
		},
	}
	cmd.Flags().Bool(previewFlag, false, "Preview mode (does not save transactions)")
	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		panic(err)
	}
	return cmd
}
