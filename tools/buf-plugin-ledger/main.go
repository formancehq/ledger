// Command buf-plugin-ledger is a buf check plugin enforcing the Ledger v3
// protobuf conventions from docs/technical/contributing/protobuf.md. Ledger v3
// is unreleased: obsolete fields are deleted and the remaining numbers are
// realigned instead of being reserved.
//
// It is declared in misc/proto/buf.yaml and runs through `buf lint`.
package main

import (
	"context"
	"slices"

	"buf.build/go/bufplugin/check"
	"buf.build/go/bufplugin/check/checkutil"
	"buf.build/go/bufplugin/descriptor"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	noReservedRuleID             = "LEDGER_NO_RESERVED"
	fieldNumbersSequentialRuleID = "LEDGER_FIELD_NUMBERS_SEQUENTIAL"
	enumNumbersSequentialRuleID  = "LEDGER_ENUM_NUMBERS_SEQUENTIAL"

	// Source path field numbers from google/protobuf/descriptor.proto.
	messageReservedRangeField = 9
	messageReservedNameField  = 10
	enumReservedRangeField    = 4
	enumReservedNameField     = 5
)

func newSpec() *check.Spec {
	return &check.Spec{
		Rules: []*check.RuleSpec{
			{
				ID:      noReservedRuleID,
				Default: true,
				Purpose: "Checks that messages and enums declare no reserved numbers or names; v3 realigns numbers instead.",
				Type:    check.RuleTypeLint,
				Handler: checkutil.NewFileRuleHandler(checkNoReserved, checkutil.WithoutImports()),
			},
			{
				ID:      fieldNumbersSequentialRuleID,
				Default: true,
				Purpose: "Checks that message field numbers are exactly 1..N, oneof members included.",
				Type:    check.RuleTypeLint,
				Handler: checkutil.NewMessageRuleHandler(checkFieldNumbersSequential, checkutil.WithoutImports()),
			},
			{
				ID:      enumNumbersSequentialRuleID,
				Default: true,
				Purpose: "Checks that enum value numbers are exactly 0..N-1.",
				Type:    check.RuleTypeLint,
				Handler: checkutil.NewEnumRuleHandler(checkEnumNumbersSequential, checkutil.WithoutImports()),
			},
		},
	}
}

func main() {
	check.Main(newSpec())
}

func checkNoReserved(
	_ context.Context,
	responseWriter check.ResponseWriter,
	_ check.Request,
	file descriptor.FileDescriptor,
) error {
	fd := file.ProtoreflectFileDescriptor()

	walkEnums := func(enums protoreflect.EnumDescriptors) {
		for i := range enums.Len() {
			enum := enums.Get(i)
			reportReserved(responseWriter, fd, enum, "enum",
				enum.ReservedRanges().Len(), enumReservedRangeField,
				enum.ReservedNames().Len(), enumReservedNameField)
		}
	}

	var walkMessages func(messages protoreflect.MessageDescriptors)
	walkMessages = func(messages protoreflect.MessageDescriptors) {
		for i := range messages.Len() {
			message := messages.Get(i)
			reportReserved(responseWriter, fd, message, "message",
				message.ReservedRanges().Len(), messageReservedRangeField,
				message.ReservedNames().Len(), messageReservedNameField)
			walkEnums(message.Enums())
			walkMessages(message.Messages())
		}
	}

	walkEnums(fd.Enums())
	walkMessages(fd.Messages())

	return nil
}

// reportReserved annotates each reserved statement, pointing at the statement
// itself rather than the enclosing declaration.
func reportReserved(
	responseWriter check.ResponseWriter,
	fd protoreflect.FileDescriptor,
	decl protoreflect.Descriptor,
	kind string,
	ranges int, rangeField int32,
	names int, nameField int32,
) {
	if ranges == 0 && names == 0 {
		return
	}

	base := fd.SourceLocations().ByDescriptor(decl).Path
	for _, entry := range []struct {
		count int
		field int32
	}{{ranges, rangeField}, {names, nameField}} {
		for i := range entry.count {
			path := slices.Concat(base, protoreflect.SourcePath{entry.field, int32(i)})
			responseWriter.AddAnnotation(
				check.WithMessagef("%s %q declares reserved entries; Ledger v3 deletes obsolete fields and realigns the remaining numbers.", kind, decl.FullName()),
				check.WithFileNameAndSourcePath(fd.Path(), path),
			)
		}
	}
}

func checkFieldNumbersSequential(
	_ context.Context,
	responseWriter check.ResponseWriter,
	_ check.Request,
	message protoreflect.MessageDescriptor,
) error {
	if message.IsMapEntry() {
		return nil
	}

	fields := message.Fields()
	numbers := make([]int, fields.Len())
	for i := range fields.Len() {
		numbers[i] = int(fields.Get(i).Number())
	}
	if missing, ok := firstMissing(numbers, 1); !ok {
		responseWriter.AddAnnotation(
			check.WithMessagef("message %q field numbers must be 1..%d without gaps; number %d is missing.", message.FullName(), len(numbers), missing),
			check.WithDescriptor(message),
		)
	}

	return nil
}

func checkEnumNumbersSequential(
	_ context.Context,
	responseWriter check.ResponseWriter,
	_ check.Request,
	enum protoreflect.EnumDescriptor,
) error {
	values := enum.Values()
	numbers := make([]int, values.Len())
	for i := range values.Len() {
		numbers[i] = int(values.Get(i).Number())
	}
	if missing, ok := firstMissing(numbers, 0); !ok {
		responseWriter.AddAnnotation(
			check.WithMessagef("enum %q value numbers must be 0..%d without gaps or aliases; number %d is missing.", enum.FullName(), len(numbers)-1, missing),
			check.WithDescriptor(enum),
		)
	}

	return nil
}

// firstMissing reports whether numbers is a permutation of start..start+n-1
// and, when it is not, the first number of that range absent from numbers.
// A duplicate (enum alias) necessarily leaves one number of the range absent.
func firstMissing(numbers []int, start int) (int, bool) {
	present := make(map[int]struct{}, len(numbers))
	for _, number := range numbers {
		present[number] = struct{}{}
	}
	for i := range len(numbers) {
		if _, ok := present[start+i]; !ok {
			return start + i, false
		}
	}

	return 0, true
}
