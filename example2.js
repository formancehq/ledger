function next() {
    let postings = [];
    for(let i = 0; i < 500000; i++) {
        postings.push({
            source: `world`,
            destination: `banks`,
            amount: 100,
            asset: 'USD'
        })
    }
    return {
        action: 'CREATE_TRANSACTION',
        data: {
            postings
        }
    }
}



