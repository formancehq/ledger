const plain = `send [USD 100] (
	source = @world
	destination = @bank
)`

function next() {
    return [{
        action: 'CREATE_TRANSACTION',
        data: {
            script: {
                plain
            }
        }
    }]
}