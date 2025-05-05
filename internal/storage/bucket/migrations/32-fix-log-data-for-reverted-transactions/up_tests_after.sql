do $$
	declare
		expected varchar = '{"transaction": {"id": 22, "metadata": {"tax": "1%"}, "postings": [{"asset": "USD", "amount": 99, "source": "sellers:0", "destination": "orders:10"}, {"asset": "USD", "amount": 1, "source": "fees", "destination": "orders:10"}, {"asset": "USD", "amount": 100, "source": "orders:10", "destination": "world"}, {"asset": "SELL", "amount": 1, "source": "sellers:0", "destination": "world"}], ' ||
            '"timestamp": "' ||
            (select to_json(timestamp)#>>'{}' from "{{.Schema}}".transactions where id = 22 and ledger = 'ledger0')
            || 'Z"}, "revertedTransaction": {"id": 2, "metadata": {"tax": "1%"}, "postings": [{"asset": "SELL", "amount": 1, "source": "world", "destination": "sellers:0"}, {"asset": "USD", "amount": 100, "source": "world", "destination": "orders:10"}, {"asset": "USD", "amount": 1, "source": "orders:10", "destination": "fees"}, {"asset": "USD", "amount": 99, "source": "orders:10", "destination": "sellers:0"}], "reverted": true, "reference": null, "timestamp": ' ||
		    (select to_json(timestamp) from "{{.Schema}}".transactions where id = 2 and ledger = 'ledger0') ||
            ', "insertedAt": ' ||
            (select to_json(inserted_at) from "{{.Schema}}".transactions where id = 2 and ledger = 'ledger0') ||
		    ', "revertedAt": ' ||
            (select to_json(reverted_at) from "{{.Schema}}".transactions where id = 2 and ledger = 'ledger0') ||
            ', "postCommitVolumes": {"fees": {"USD": {"input": 3, "output": 0}}, "world": {"USD": {"input": 0, "output": 300}, "SELL": {"input": 0, "output": 3}}, "orders:10": {"USD": {"input": 100, "output": 100}}, "sellers:0": {"USD": {"input": 297, "output": 0}, "SELL": {"input": 3, "output": 0}}}}, "revertedTransactionID": "2"}';
	begin
		set search_path = '{{.Schema}}';
		assert (select data::varchar from logs where id = 22 and ledger = 'ledger0') = expected,
 			'data should be equals to ' || expected || ' but was ' || (select data::varchar from logs where id = 22 and ledger = 'ledger0');
	end;
$$