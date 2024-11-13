function next() {
    return {
        action: 'CREATE_TRANSACTION',
        data: {
            plain: `vars {
                account $source
                account $destination
            }
            send [USD/2 100] (
                source = $source allowing unbounded overdraft
                destination = $destination
            )`,
            vars: {
                destination: "dst:" + uuid(),
                source: "src:" + uuid()
            }
        }
    }
}