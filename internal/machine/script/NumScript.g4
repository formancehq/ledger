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

monetary: LBRACK asset=expression amt=NUMBER RBRACK;

monetaryAll: LBRACK asset=expression '*' RBRACK;

literal
    : ACCOUNT # LitAccount
    | ASSET # LitAsset
    | NUMBER # LitNumber
    | STRING # LitString
    | PORTION # LitPortion
    | monetary # LitMonetary
    ;

variable: VARIABLE_NAME;

expression
    : lhs=expression op=(OP_ADD|OP_SUB) rhs=expression # ExprAddSub
    | lit=literal # ExprLiteral
    | var_=variable # ExprVariable
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
    | SEND (mon=expression | monAll=monetaryAll) LPAREN NEWLINE
        ( SOURCE '=' src=valueAwareSource NEWLINE DESTINATION '=' dest=destination
        | DESTINATION '=' dest=destination NEWLINE SOURCE '=' src=valueAwareSource) NEWLINE RPAREN # Send
    ;

type_
    : TY_ACCOUNT
    | TY_ASSET
    | TY_NUMBER
    | TY_STRING
    | TY_MONETARY
    | TY_PORTION
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
