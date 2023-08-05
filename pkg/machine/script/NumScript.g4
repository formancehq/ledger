grammar NumScript;

NEWLINE: [\r\n]+;
WHITESPACE: [ \t]+ -> skip;

MULTILINE_COMMENT: '/*' (MULTILINE_COMMENT|.)*? '*/' -> skip;
LINE_COMMENT: '//' .*? NEWLINE -> skip;
VARS: 'vars';
META: 'meta';
SET_TX_META: 'set_tx_meta';
SET_ACCOUNT_META: 'set_account_meta';
PRINT: 'print';
FAIL: 'fail';
SEND: 'send';
SOURCE: 'source';
FROM: 'from';
MAX: 'max';
DESTINATION: 'destination';
TO: 'to';
ALLOCATE: 'allocate';
OP_ADD: '+';
OP_SUB: '-';
OP_EQ: '==';
OP_NEQ: '!=';
OP_LT: '<';
OP_LTE: '<=';
OP_GT: '>';
OP_GTE: '>=';
OP_NOT: '!';
OP_AND: '&&';
OP_OR: '||';

LPAREN: '(';
RPAREN: ')';
LBRACK: '[';
RBRACK: ']';
LBRACE: '{';
RBRACE: '}';
EQ: '=';

TY_ACCOUNT: 'account';
TY_ASSET: 'asset';
TY_NUMBER: 'number';
TY_MONETARY: 'monetary';
TY_PORTION: 'portion';
TY_STRING: 'string';
TY_BOOL: 'bool';
STRING: '"' [a-zA-Z0-9_\- ]* '"';
PORTION:
    ( [0-9]+ [ ]? '/' [ ]? [0-9]+
    | [0-9]+     ('.'      [0-9]+)? '%'
    );
REMAINING: 'remaining';
KEPT: 'kept';
BALANCE: 'balance';
SAVE: 'save';
NUMBER: [0-9]+;
PERCENT: '%';
VARIABLE_NAME: '$' [a-z_]+ [a-z0-9_]*;
ACCOUNT: '@' [a-zA-Z_]+ [a-zA-Z0-9_:]*;
ASSET: [A-Z/0-9]+;

monetary: LBRACK asset=expression amt=expression RBRACK;

monetaryAll: LBRACK asset=expression '*' RBRACK;

literal
    : ACCOUNT # LitAccount
    | ASSET # LitAsset
    | NUMBER # LitNumber
    | STRING # LitString
    | PORTION # LitPortion
    ;

variable: VARIABLE_NAME;

expression
    : lhs=expression op=(OP_ADD | OP_SUB) rhs=expression # ExprAddSub
    | lhs=expression op=(OP_EQ | OP_NEQ | OP_LT | OP_LTE | OP_GT | OP_GTE) rhs=expression # ExprArithmeticCondition
    | OP_NOT lhs=expression # ExprLogicalNot
    | lhs=expression op=OP_AND rhs=expression # ExprLogicalAnd
    |lhs=expression op=OP_OR rhs=expression # ExprLogicalOr
    | lit=literal # ExprLiteral
    | var_=variable # ExprVariable
    | mon=monetary # ExprMonetaryNew
    | cond=expression '?' ifTrue=expression ':' ifFalse=expression # ExprTernary
    | LPAREN expr=expression RPAREN # ExprEnclosed
    ;

allotmentPortion
    : PORTION # AllotmentPortionConst
    | por=variable # AllotmentPortionVar
    | REMAINING # AllotmentPortionRemaining
    ;

destinationInOrder
    : LBRACE NEWLINE
        (MAX amounts+=expression dests+=keptOrDestination NEWLINE)+
        REMAINING remainingDest=keptOrDestination NEWLINE
    RBRACE
    ;

destinationAllotment
    : LBRACE NEWLINE
        (portions+=allotmentPortion dests+=keptOrDestination NEWLINE)+
    RBRACE
    ;

keptOrDestination
    : TO destination # IsDestination
    | KEPT # IsKept
    ;

destination
    : expression # DestAccount
    | destinationInOrder # DestInOrder
    | destinationAllotment # DestAllotment
    ;

sourceAccountOverdraft
    : 'allowing overdraft up to' specific=expression # SrcAccountOverdraftSpecific
    | 'allowing unbounded overdraft' # SrcAccountOverdraftUnbounded
    ;

sourceAccount: account=expression (overdraft=sourceAccountOverdraft)?;

sourceInOrder
    : LBRACE NEWLINE
        (sources+=source NEWLINE)+
    RBRACE
    ;

sourceMaxed: MAX max=expression FROM src=source;

source
    : sourceAccount # SrcAccount
    | sourceMaxed # SrcMaxed
    | sourceInOrder # SrcInOrder
    ;

sourceAllotment
    : LBRACE NEWLINE
        (portions+=allotmentPortion FROM sources+=source NEWLINE)+
    RBRACE
    ;

valueAwareSource
    : source # Src
    | sourceAllotment # SrcAllotment
    ;

statement
    : PRINT expr=expression # Print
    | SAVE (mon=expression | monAll=monetaryAll) FROM acc=expression # SaveFromAccount
    | SET_TX_META '(' key=STRING ',' value=expression ')' # SetTxMeta
    | SET_ACCOUNT_META '(' acc=expression ',' key=STRING ',' value=expression ')' # SetAccountMeta
    | FAIL # Fail
    | SEND mon=expression LPAREN NEWLINE
        ( SOURCE '=' src=valueAwareSource NEWLINE DESTINATION '=' dest=destination
        | DESTINATION '=' dest=destination NEWLINE SOURCE '=' src=valueAwareSource) NEWLINE RPAREN # Send
    | SEND monAll=monetaryAll LPAREN NEWLINE
        ( SOURCE '=' src=source NEWLINE DESTINATION '=' dest=destination
        | DESTINATION '=' dest=destination NEWLINE SOURCE '=' src=source) NEWLINE RPAREN # SendAll
    ;

type_
    : TY_ACCOUNT
    | TY_ASSET
    | TY_NUMBER
    | TY_STRING
    | TY_MONETARY
    | TY_PORTION
    | TY_BOOL
    ;

origin
    : META '(' account=expression ',' key=STRING ')' # OriginAccountMeta
    | BALANCE '(' account=expression ',' asset=expression ')' # OriginAccountBalance
    ;

varDecl: ty=type_ name=variable (EQ orig=origin)?;

varListDecl
    : VARS LBRACE NEWLINE
        (v+=varDecl NEWLINE+)+
    RBRACE NEWLINE
    ;

script:
    NEWLINE*
    vars=varListDecl?
    stmts+=statement
    (NEWLINE stmts+=statement)*
    NEWLINE*
    EOF
    ;
