package logging

import (
	"bufio"
	"io"

	"github.com/pkg/errors"
)

func StreamReader(logger Logger, r io.Reader, fn func(Logger, ...any)) {
	br := bufio.NewReader(r)
	for {
		// todo: handle isPrefix for lines bigger than 65 characters
		line, _, err := br.ReadLine()
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				return
			default:
				logger.Errorf("error reading container logs buffer: %s", err)
			}
			return
		}
		fn(logger, string(line))
	}
}
