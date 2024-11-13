function next() {
    return {
        action: 'CREATE_TRANSACTION',
        data: {
            plain: `vars {
                account $destination
            }
            send [USD/2 100] (
                source = @world
                destination = $destination
            )`,
            vars: {
                destination: "dst:" + uuid()
            }
        }
    }
}



