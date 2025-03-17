function next() {
    return [
        {
            action: 'CREATE_TRANSACTION',
            data: {
                script: {
                    plain: `vars {
                        account $source
                    }
                    send [USD/2 100] (
                        source = $source allowing unbounded overdraft
                        destination = @bank
                    )`,
                    vars: {
                        source: "src:" + uuid()
                    }
                }
            }
        }
    ]
}