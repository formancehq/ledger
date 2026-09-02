package main

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidationReceiptKeyRequiresExactSemanticIdentity(t *testing.T) {
	t.Parallel()

	baseline := completeValidationReceiptKey()
	testCases := []struct {
		name     string
		expected string
		mutate   func(*validationReceiptKey)
	}{
		{name: "receipt version", expected: "version", mutate: func(key *validationReceiptKey) { key.Version = "ledger-validation-receipt-v2" }},
		{name: "base ref", expected: "baseRef", mutate: func(key *validationReceiptKey) { key.BaseRef = "refs/heads/other" }},
		{name: "base SHA", expected: "baseSha", mutate: func(key *validationReceiptKey) { key.BaseSHA = "base-2" }},
		{name: "candidate worktree", expected: "candidateWorktree", mutate: func(key *validationReceiptKey) { key.CandidateWorktree = "/candidate-2" }},
		{name: "candidate HEAD", expected: "candidateHead", mutate: func(key *validationReceiptKey) { key.CandidateHead = "head-2" }},
		{name: "candidate file", expected: "candidateFingerprint", mutate: func(key *validationReceiptKey) { key.CandidateFingerprint = "candidate-2" }},
		{name: "candidate ignored generated input", expected: "candidateRootFingerprint", mutate: func(key *validationReceiptKey) { key.CandidateRootFingerprint = "candidate-root-2" }},
		{name: "trusted root checkout", expected: "trustedRootCheckout", mutate: func(key *validationReceiptKey) { key.TrustedRootCheckout = "/root-2" }},
		{name: "trusted root identity", expected: "trustedRootFingerprint", mutate: func(key *validationReceiptKey) { key.TrustedRootFingerprint = "root-2" }},
		{name: "trusted tool worktree", expected: "trustedToolWorktree", mutate: func(key *validationReceiptKey) { key.TrustedToolWorktree = "/tool-2" }},
		{name: "trusted tool SHA", expected: "trustedToolHead", mutate: func(key *validationReceiptKey) { key.TrustedToolHead = "tool-head-2" }},
		{name: "trusted tool content", expected: "trustedToolFingerprint", mutate: func(key *validationReceiptKey) { key.TrustedToolFingerprint = "tool-2" }},
		{name: "validation command", expected: "validationCommand", mutate: func(key *validationReceiptKey) { key.ValidationCommand = "validate --other" }},
		{name: "gate selector", expected: "validationGatesCommand", mutate: func(key *validationReceiptKey) { key.ValidationGatesCommand = "list-other" }},
		{name: "selected gates", expected: "selectedGates", mutate: func(key *validationReceiptKey) { key.SelectedGatesFingerprint = "gates-2" }},
		{name: "environment contract and build options", expected: "validationEnvironment", mutate: func(key *validationReceiptKey) { key.ValidationEnvironment = "env-with-other-goflags" }},
		{name: "validation run directory", expected: "validationRunDirectory", mutate: func(key *validationReceiptKey) { key.ValidationRunDirectory = "/validation-2" }},
		{name: "review state directory", expected: "reviewStateDirectory", mutate: func(key *validationReceiptKey) { key.ReviewStateDirectory = "/state-2" }},
		{name: "binding file", expected: "worktreeBindingFile", mutate: func(key *validationReceiptKey) { key.WorktreeBindingFile = "/binding-2" }},
		{name: "binding content", expected: "worktreeBinding", mutate: func(key *validationReceiptKey) { key.WorktreeBindingFingerprint = "binding-2" }},
		{name: "Git guard", expected: "gitGuard", mutate: func(key *validationReceiptKey) { key.GitGuardFingerprint = "guard-2" }},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			current := baseline
			testCase.mutate(&current)
			require.Equal(t, []string{testCase.expected}, baseline.mismatches(current))
			require.NotEqual(t, validationReceiptDigest(baseline), validationReceiptDigest(current))
		})
	}
}

func TestValidationReceiptCachesAreRunLocal(t *testing.T) {
	t.Parallel()

	key := completeValidationReceiptKey()
	first := &validationReceiptCache{receipt: &validationReceipt{key: key}}
	second := &validationReceiptCache{}
	require.NotNil(t, first.receipt)
	require.Nil(t, second.receipt, "a new review-loop process starts without a receipt")

	const runs = 32
	caches := make([]*validationReceiptCache, runs)
	var waitGroup sync.WaitGroup
	for index := range caches {
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			localKey := key
			localKey.CandidateWorktree += string(rune('a' + index))
			caches[index] = &validationReceiptCache{receipt: &validationReceipt{key: localKey}}
		}(index)
	}
	waitGroup.Wait()

	for left := range caches {
		for right := left + 1; right < len(caches); right++ {
			require.NotSame(t, caches[left], caches[right])
			require.NotEqual(t, caches[left].receipt.key.CandidateWorktree, caches[right].receipt.key.CandidateWorktree)
		}
	}
}

func TestValidationReceiptExactMatchHasNoMismatches(t *testing.T) {
	t.Parallel()

	key := completeValidationReceiptKey()
	require.Empty(t, key.mismatches(key))
	require.Equal(t, validationReceiptDigest(key), validationReceiptDigest(key))
}

func TestValidationEnvironmentIdentityIsOrderIndependentAndRejectsDuplicates(t *testing.T) {
	t.Parallel()

	first, err := environmentFingerprint([]string{"GOFLAGS=-tags=one", "PATH=/trusted/bin"})
	require.NoError(t, err)
	second, err := environmentFingerprint([]string{"PATH=/trusted/bin", "GOFLAGS=-tags=one"})
	require.NoError(t, err)
	require.Equal(t, first, second)

	_, err = environmentFingerprint([]string{"GOFLAGS=-tags=one", "GOFLAGS=-tags=two"})
	require.ErrorContains(t, err, "duplicate variable")
}

func completeValidationReceiptKey() validationReceiptKey {
	return validationReceiptKey{
		Version:                    validationReceiptVersion,
		BaseRef:                    "refs/heads/release/v3.0",
		BaseSHA:                    "base",
		CandidateWorktree:          "/candidate",
		CandidateHead:              "head",
		CandidateFingerprint:       "candidate",
		CandidateRootFingerprint:   "candidate-root",
		TrustedRootCheckout:        "/root",
		TrustedRootFingerprint:     "root",
		TrustedToolWorktree:        "/tool",
		TrustedToolHead:            "tool-head",
		TrustedToolFingerprint:     "tool",
		ValidationCommand:          "validate",
		ValidationGatesCommand:     "list-gates",
		SelectedGatesFingerprint:   "gates",
		ValidationEnvironment:      "environment",
		ValidationRunDirectory:     "/validation",
		ReviewStateDirectory:       "/state",
		WorktreeBindingFile:        "/binding",
		WorktreeBindingFingerprint: "binding",
		GitGuardFingerprint:        "guard",
	}
}
