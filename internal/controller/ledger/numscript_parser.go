package ledger

import (
	"crypto/sha256"
	"encoding/base64"

	"github.com/bluele/gcache"
	"github.com/formancehq/ledger/internal/machine/script/compiler"
	"github.com/formancehq/numscript"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source numscript_parser.go -destination numscript_parser_generated_test.go -package ledger . NumscriptParser

type NumscriptParser interface {
	// Parse can return following errors:
	//  * ErrCompilationFailed
	Parse(script string) (NumscriptRuntime, error)
}

type DefaultNumscriptParser struct{}

func (d *DefaultNumscriptParser) Parse(script string) (NumscriptRuntime, error) {
	ret, err := compiler.Compile(script)
	if err != nil {
		return nil, newErrCompilationFailed(err)
	}
	return NewMachineNumscriptRuntimeAdapter(*ret), nil
}

func NewDefaultNumscriptParser() *DefaultNumscriptParser {
	return &DefaultNumscriptParser{}
}

var _ NumscriptParser = (*DefaultNumscriptParser)(nil)

type InterpreterNumscriptParser struct{ featureFlags []string }

func (n *InterpreterNumscriptParser) Parse(script string) (NumscriptRuntime, error) {
	result := numscript.Parse(script)
	errs := result.GetParsingErrors()
	if len(errs) != 0 {
		return nil, ErrParsing{
			Source: script,
			Errors: errs,
		}
	}

	interpreterFlags := make(map[string]struct{})
	for _, flag := range n.featureFlags {
		interpreterFlags[flag] = struct{}{}
	}
	return NewDefaultInterpreterMachineAdapter(result, interpreterFlags), nil
}

func NewInterpreterNumscriptParser(featureFlags []string) *InterpreterNumscriptParser {
	return &InterpreterNumscriptParser{
		featureFlags: featureFlags,
	}
}

var _ NumscriptParser = (*InterpreterNumscriptParser)(nil)

type CacheConfiguration struct {
	MaxCount uint
}

type CachedParser struct {
	underlying NumscriptParser
	cache      gcache.Cache
}

func (c *CachedParser) Parse(script string) (NumscriptRuntime, error) {
	digest := sha256.New()
	_, err := digest.Write([]byte(script))
	if err != nil {
		return nil, err
	}

	cacheKey := base64.StdEncoding.EncodeToString(digest.Sum(nil))
	v, err := c.cache.Get(cacheKey)
	if err == nil {
		return v.(NumscriptRuntime), nil
	}

	program, err := c.underlying.Parse(script)
	if err != nil {
		return nil, err
	}

	_ = c.cache.Set(cacheKey, program)

	return program, nil
}

func NewCachedNumscriptParser(parser NumscriptParser, configuration CacheConfiguration) *CachedParser {
	return &CachedParser{
		underlying: parser,
		cache:      gcache.New(int(configuration.MaxCount)).LFU().Build(),
	}
}

var _ NumscriptParser = (*CachedParser)(nil)
