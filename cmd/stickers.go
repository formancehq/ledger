package cmd

import (
	"fmt"
	"os/exec"
	"runtime"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func open(url string) {
	var err error

	switch runtime.GOOS {
	case "linux":
		err = exec.Command("xdg-open", url).Start()
	case "windows":
		err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		err = exec.Command("open", url).Start()
	default:
		fmt.Printf("you should head to: %s\n", url)
	}

	if err != nil {
		logrus.Fatal(err)
	}
}

var stickersCmd = &cobra.Command{
	Use: "stickers",
	Run: func(cmd *cobra.Command, args []string) {
		token := fmt.Sprintf("cli-%d", time.Now().Unix())
		url := fmt.Sprintf("https://airtable.com/shrp41dAnjv0LSlxW?prefill_Token=%s", token)

		fmt.Printf("You found a very special sub-command...\n\n")
		fmt.Printf("Hit Enter to continue\n\n")
		_, err := fmt.Scanln()
		if err != nil {
			panic(err)
		}
		open(url)
	},
}
