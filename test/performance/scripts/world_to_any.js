function next() {
    return {
        script: `vars {
            account $destination
        }
        send [USD/2 100] (
            source = @world
            destination = $destination
        )`,
        variables: {
            destination: "dst:" + uuid()
        }
    }
}



