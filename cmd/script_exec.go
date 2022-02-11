package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/numary/ledger/pkg/api/controllers"
	"io/ioutil"
	"net/http"
	"regexp"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func NewScriptExec() *cobra.Command {
	return &cobra.Command{
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

			res, err := http.Post(
				fmt.Sprintf(
					"http://%s/%s/script",
					viper.Get(serverHttpBindAddressFlag),
					args[0],
				),
				"application/json",
				bytes.NewReader(b),
			)
			if err != nil {
				logrus.Fatal(err)
			}

			result := controllers.ScriptResponse{}
			err = json.NewDecoder(res.Body).Decode(&result)
			if err != nil {
				logrus.Fatal(err)
			}

			if result.ErrorCode != "" {
				logrus.Fatal(result.ErrorCode, result.ErrorMessage)
			}

			fmt.Println("Script ran successfully ✅")
			fmt.Printf("Created transaction: http://%s/%s/transactions/%d\r\n",
				viper.Get(serverHttpBindAddressFlag),
				args[0],
				result.Transaction.ID,
			)
		},
	}
}
