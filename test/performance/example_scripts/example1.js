const script = `vars {
    account $order
    account $seller
}
send [USD/2 100] (
    source = @world
    destination = $order
)
send [USD/2 1] (
    source = $order
    destination = @fees
)
send [USD/2 99] (
    source = $order
    destination = $seller
)`

function next(iteration) {
    return {
        script,
        variables: {
            order: `orders:${uuid()}`,
            seller: `sellers:${iteration % 5}`
        }
    }
}



