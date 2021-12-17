package cmd

import (
	"embed"
	"fmt"
	"net/http"
	"os/exec"
	"regexp"
	"runtime"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

//go:embed control
var uipath embed.FS

func openuri(uri string) bool {
	var err error

	switch runtime.GOOS {
	case "linux":
		err = exec.Command("xdg-open", uri).Start()
	case "windows":
		err = exec.Command("rundll32", "url.dll,FileProtocolHandler", uri).Start()
	case "darwin":
		err = exec.Command("open", uri).Start()
	default:
		err = fmt.Errorf("unsupported platform, open manually: %s", uri)
	}

	return err != nil
}

var UICmd = &cobra.Command{
	Use: "ui",
	Run: func(cmd *cobra.Command, args []string) {
		addr := viper.GetString("ui.http.bind_address")

		handler := http.FileServer(http.FS(uipath))

		http.HandleFunc("/", func(rw http.ResponseWriter, r *http.Request) {
			isFile := regexp.MustCompile(`\.[a-z]{2,}$`)
			path := r.URL.Path
			if !isFile.MatchString(path) {
				path = "/"
			}
			r.URL.Path = fmt.Sprintf("/control%s", path)

			handler.ServeHTTP(rw, r)
		})

		openuri(addr)
		fmt.Printf("Numary control is live on http://%s\n", addr)

		http.ListenAndServe(addr, nil)
	},
}
