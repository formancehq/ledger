package cmd

import (
	"embed"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"runtime"

	"github.com/spf13/cobra"
)

//go:embed control/index.html control/app.js
var uipath embed.FS

func openuri(uri string) {
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

	if err != nil {
		log.Fatal(err)
	}
}

var UICmd = &cobra.Command{
	Use: "ui",
	Run: func(cmd *cobra.Command, args []string) {
		tmp := os.TempDir()
		dir := path.Join(tmp, "numary-ui")
		os.Mkdir(dir, 0700)
		os.Chdir(dir)

		// os.WriteFile("index.html", []byte(`
		// 	<html>
		// 		<head></head>
		// 		<body>
		// 			<h1>coming soon</h1>
		// 		</body>
		// 	</html>
		// `), 0644)

		openuri("localhost:3078")

		handler := http.FileServer(http.FS(uipath))

		http.HandleFunc("/", func(rw http.ResponseWriter, r *http.Request) {
			r.URL.Path = fmt.Sprintf("/control%s", r.URL.Path)
			handler.ServeHTTP(rw, r)
		})

		http.ListenAndServe("localhost:3078", nil)
	},
}
