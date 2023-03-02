package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"

	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
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
		RunE: func(cmd *cobra.Command, args []string) error {
			b, err := os.ReadFile(args[1])
			if err != nil {
				return err
			}

			r := regexp.MustCompile(`^\n`)
			s := string(b)
			s = r.ReplaceAllString(s, "")

			b, err = json.Marshal(gin.H{
				"plain": s,
			})
			if err != nil {
				return err
			}

			logrus.Debugln(string(b))

			req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/%s/script",
				viper.Get(bindFlag),
				args[0]), bytes.NewReader(b))
			if err != nil {
				return err
			}

			values := url.Values{}
			if viper.GetBool(previewFlag) {
				values.Set("preview", "yes")
			}
			req.URL.RawQuery = values.Encode()
			req.Header.Set("Content-Type", "application/json")

			res, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}

			result := controllers.ScriptResponse{}
			err = json.NewDecoder(res.Body).Decode(&result)
			if err != nil {
				return err
			}

			if result.ErrorCode != "" {
				switch result.ErrorCode {
				case "INTERNAL":
					return errors.New("unexpected error occurred")
				default:
					return fmt.Errorf("unexpected error: %s", result.ErrorMessage)
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
					p.Amount,
					p.Asset,
				)
			}
			if !viper.GetBool(previewFlag) {
				fmt.Printf("Created transaction: http://%s/%s/transactions/%d\r\n",
					viper.Get(bindFlag),
					args[0],
					result.Transaction.ID)
			}
			return nil
		},
	}
	cmd.Flags().Bool(previewFlag, false, "Preview mode (does not save transactions)")
	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		panic(err)
	}
	return cmd
}
