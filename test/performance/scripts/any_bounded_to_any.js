function next() {
    return {
        action: 'CREATE_TRANSACTION',
        data: {
            plain: `vars {
                account $source
                account $destination
            }
            send [USD/2 100] (
                source = $source allowing overdraft up to [USD/2 100]
                destination = $destination
            )`,
            vars: {
                destination: "dst:" + uuid(),
                source: "src:" + uuid()
            }
        }
    }
}