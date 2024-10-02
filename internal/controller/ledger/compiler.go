package ledger

import (
	"crypto/sha256"
	"encoding/base64"

	"github.com/bluele/gcache"
	"github.com/formancehq/ledger/internal/machine/script/compiler"
	"github.com/formancehq/ledger/internal/machine/vm/program"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source compiler.go -destination compiler_generated_test.go -package ledger . Compiler

// Compiler can return following errors:
//   - ErrCompilationFailed
type Compiler interface {
	Compile(script string) (*program.Program, error)
}
type CompilerFn func(script string) (*program.Program, error)

func (fn CompilerFn) Compile(script string) (*program.Program, error) {
	return fn(script)
}

func NewDefaultCompiler() CompilerFn {
	return func(script string) (*program.Program, error) {
		ret, err := compiler.Compile(script)
		if err != nil {
			return nil, newErrCompilationFailed(err)
		}
		return ret, nil
	}
}

type CacheConfiguration struct {
	MaxCount uint
}

type CachedCompiler struct {
	underlying Compiler
	cache      gcache.Cache
}

func (c *CachedCompiler) Compile(script string) (*program.Program, error) {

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

	program, err := c.underlying.Compile(script)
	if err != nil {
		return nil, err
	}

	_ = c.cache.Set(cacheKey, program)

	return program, nil
}

func NewCachedCompiler(compiler Compiler, configuration CacheConfiguration) *CachedCompiler {
	return &CachedCompiler{
		underlying: compiler,
		cache:      gcache.New(int(configuration.MaxCount)).LFU().Build(),
	}
}

var _ Compiler = (*CachedCompiler)(nil)
