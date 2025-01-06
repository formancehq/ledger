function next() {
    return [
        {
            action: 'CREATE_TRANSACTION',
            data: {
                script: {
                    plain: `send [USD/2 100] (
                        source = @world
                        destination = @bank
                    )`,
                    vars: {}
                }
            }
        }
    ]
}