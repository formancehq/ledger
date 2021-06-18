package cmd

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"runtime"

	"github.com/spf13/cobra"
)

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

		os.WriteFile("index.html", []byte(`
			<html>
				<head></head>
				<body>
					<h1>coming soon</h1>
				</body>
			</html>
		`), 0644)

		openuri(path.Join(dir, "index.html"))
	},
}
