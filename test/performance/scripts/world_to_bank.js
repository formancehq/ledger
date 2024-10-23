function next() {
    return {
        script: `send [USD/2 100] (
            source = @world
            destination = @bank
        )`,
        variables: {}
    }
}