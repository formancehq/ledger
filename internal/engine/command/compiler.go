package command

import (
	"crypto/sha256"
	"encoding/base64"

	"github.com/bluele/gcache"
	"github.com/formancehq/ledger/v2/internal/machine/script/compiler"
	"github.com/formancehq/ledger/v2/internal/machine/vm/program"
)

type Compiler struct {
	cache gcache.Cache
}

func (c *Compiler) Compile(script string) (*program.Program, error) {

	digest := sha256.New()
	_, err := digest.Write([]byte(script))
	if err != nil {
		return nil, err
	}

	cacheKey := base64.StdEncoding.EncodeToString(digest.Sum(nil))
	v, err := c.cache.Get(cacheKey)
	if err == nil {
		return v.(*program.Program), nil
	}

	program, err := compiler.Compile(script)
	if err != nil {
		return nil, err
	}
	_ = c.cache.Set(cacheKey, program)

	return program, nil
}

func NewCompiler(maxCacheCount int) *Compiler {
	return &Compiler{
		cache: gcache.New(maxCacheCount).
			LFU().
			Build(),
	}
}
