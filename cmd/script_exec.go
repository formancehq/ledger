package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
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
				"plain": string(s),
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
				bytes.NewReader([]byte(b)),
			)
			if err != nil {
				logrus.Fatal(err)
			}

			b, err = ioutil.ReadAll(res.Body)
			if err != nil {
				logrus.Fatal(err)
			}

			var result struct {
				Err string `json:"err,omitempty"`
				Ok  bool   `json:"ok"`
			}
			err = json.Unmarshal(b, &result)
			if err != nil {
				logrus.Fatal(err)
			}

			if result.Err != "" {
				logrus.Fatal(result.Err)
			} else {
				fmt.Println("Script ran successfully ✅")
			}
		},
	}
}
