function next() {
    return {
        script: `vars {
            account $source
            account $destination
        }
        send [USD/2 100] (
            source = $source allowing unbounded overdraft
            destination = $destination
        )`,
        variables: {
            destination: "dst:" + uuid(),
            source: "src:" + uuid()
        }
    }
}