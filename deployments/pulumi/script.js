const nbPsps = 10;
const nbOrganizations = 100;
const nbSellers = 1000;
const nbUsers = 10000;
const nbAssets = 3;

const plain = `
vars {
    account $order
    account $buyer
    account $seller
    account $psp
    account $fees_account
    
    monetary $amount
    portion $fees
    string $due_date
    string $status
}

send $amount (
    source = $psp allowing unbounded overdraft
    destination = $buyer
)

send $amount (
    source = $buyer
    destination = $order
)

send $amount (
    source = $order
    destination = {
        $fees to $fees_account
        remaining to $seller
    }
)

set_account_meta($order, "due_date", $due_date)
set_tx_meta("status", $status)
`

function next(iteration) {
    const dueDate = new Date();
    const offset = Math.floor((Math.random() - 0.5) * 100000);
    dueDate.setSeconds(dueDate.getSeconds() + offset);

    const orderID = uuid();
    const organizationID = Math.floor(Math.random() * nbOrganizations);
    const userID = Math.floor(Math.random() * nbUsers);
    const sellerID = Math.floor(Math.random() * nbSellers);

    const status = offset > 0 ? 'PENDING' : 'COMPLETED';
    const psp = `organizations:${organizationID}:psp:${Math.floor(Math.random() * nbPsps)}`;
    const order = `organizations:${organizationID}:orders:${orderID}`;
    const buyer = `organizations:${organizationID}:users:${userID}`;
    const seller = `organizations:${organizationID}:sellers:${sellerID}`;
    const amount = `ASSET${Math.floor(Math.random() * nbAssets)} ${Math.floor(Math.random() * 10000000000)}`;
    const fees = `${Math.floor(Math.random() * 10)}%`;
    const fees_account = `organizations:${organizationID}:fees`;

    return [{
        action: 'CREATE_TRANSACTION',
        data: {
            script: {
                plain,
                vars: {
                    order,
                    buyer,
                    seller,
                    psp,
                    fees_account,
                    amount,
                    fees,
                    due_date: dueDate.toISOString(),
                    status
                }
            },
            metadata: {
                "iteration": `${iteration}`
            }
        }
    }]
}