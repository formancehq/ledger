package command

import (
	"context"
	"crypto/sha256"
	"encoding/base64"

	"github.com/bluele/gcache"
	"github.com/formancehq/ledger/pkg/machine/script/compiler"
	"github.com/formancehq/ledger/pkg/machine/vm"
	"github.com/formancehq/ledger/pkg/machine/vm/program"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
)

type Compiler struct {
	cache gcache.Cache
}

func (c *Compiler) Compile(ctx context.Context, script string) (*program.Program, error) {

	digest := sha256.New()
	_, err := digest.Write([]byte(script))
	if err != nil {
		return nil, errorsutil.NewError(vm.ErrCompilationFailed, err)
	}

	cacheKey := base64.StdEncoding.EncodeToString(digest.Sum(nil))
	v, err := c.cache.Get(cacheKey)
	if err == nil {
		return v.(*program.Program), nil
	}

	program, err := compiler.Compile(script)
	if err != nil {
		return nil, errorsutil.NewError(vm.ErrCompilationFailed, err)
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
