package program

const (
	OP_APUSH            = byte(iota + 1)
	OP_BUMP             // <value_to_bump: any> <any>*N <int N> => <any>*N <value_to_bump>
	OP_DELETE           // <value: not funding>
	OP_IADD             // <number> + <number> => <number>
	OP_ISUB             // <number> - <number> => <number>
	OP_PRINT            // <any>
	OP_FAIL             //
	OP_ASSET            // <asset | monetary | funding> => <asset>
	OP_MONETARY_NEW     // <asset> <number> => <monetary>
	OP_MONETARY_ADD     // <monetary> + <monetary> => <monetary>   // panics if not same asset
	OP_MONETARY_SUB     // <monetary> - <monetary> => <monetary>   // panics if not same asset
	OP_MAKE_ALLOTMENT   // <portion>*N <int N> => <allotment(N)>
	OP_TAKE_ALL         // <source: account> <overdraft: monetary> => <funding>
	OP_TAKE_ALWAYS      // <source: account> <monetary> => <funding>   // takes amount from account unconditionally
	OP_TAKE             // <funding> <monetary> => <remaining: funding> <taken: funding> // fails with EXIT_INSUFFICIENT_FUNDS if not enough
	OP_TAKE_MAX         // <funding> <monetary> => <missing: monetary> <remaining: funding> <taken: funding> // Doesn't fail on insufficient funds. Either missing or remaining is zero.
	OP_FUNDING_ASSEMBLE // <funding>*N <int N> => <funding> (first has highest priority)
	OP_FUNDING_SUM      // <funding> => <funding> <sum: monetary>
	OP_FUNDING_REVERSE  // <funding> => <funding>
	OP_REPAY            // <funding>
	OP_ALLOC            // <monetary> <allotment(N)> => <monetary>*N
	OP_SEND             // <funding> <account>
	OP_TX_META          //
	OP_ACCOUNT_META     //
	OP_SAVE
)

func OpcodeName(op byte) string {
	switch op {
	case OP_APUSH:
		return "OP_APUSH"
	case OP_BUMP:
		return "OP_BUMP"
	case OP_DELETE:
		return "OP_DELETE"
	case OP_IADD:
		return "OP_IADD"
	case OP_ISUB:
		return "OP_ISUB"
	case OP_PRINT:
		return "OP_PRINT"
	case OP_FAIL:
		return "OP_FAIL"
	case OP_ASSET:
		return "OP_ASSET"
	case OP_MONETARY_NEW:
		return "OP_MONETARY_NEW"
	case OP_MONETARY_ADD:
		return "OP_MONETARY_ADD"
	case OP_MONETARY_SUB:
		return "OP_MONETARY_SUB"
	case OP_MAKE_ALLOTMENT:
		return "OP_MAKE_ALLOTMENT"
	case OP_TAKE_ALL:
		return "OP_TAKE_ALL"
	case OP_TAKE_ALWAYS:
		return "OP_TAKE_ALWAYS"
	case OP_TAKE:
		return "OP_TAKE"
	case OP_TAKE_MAX:
		return "OP_TAKE_MAX"
	case OP_FUNDING_ASSEMBLE:
		return "OP_FUNDING_ASSEMBLE"
	case OP_FUNDING_SUM:
		return "OP_FUNDING_SUM"
	case OP_FUNDING_REVERSE:
		return "OP_FUNDING_REVERSE"
	case OP_REPAY:
		return "OP_REPAY"
	case OP_ALLOC:
		return "OP_ALLOC"
	case OP_SEND:
		return "OP_SEND"
	case OP_TX_META:
		return "OP_TX_META"
	case OP_ACCOUNT_META:
		return "OP_ACCOUNT_META"
	case OP_SAVE:
		return "OP_SAVE"
	default:
		return "Unknown opcode"
	}
}
