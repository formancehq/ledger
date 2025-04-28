set search_path = '{{.Schema}}';

create extension if not exists "uuid-ossp";

insert into logs(seq, ledger, id, type, date, data, hash)
select
	seq,
	'ledger' || seq % 5,
	(seq/5) + (seq % 5),
	'NEW_TRANSACTION',
	now(),
	('{'
		'"transaction": {'
			'"id": ' || (seq/5) + (seq % 5) || ','
			'"timestamp": ' || to_json(now()::timestamp without time zone) || ','
			'"postings": ['
				'{'
					'"destination": "sellers:' || (seq % 5) || '",'
		            '"source": "world",'
		            '"asset": "SELL",'
		            '"amount": 1'
		        '},'
	            '{'
					'"source": "world",'
					'"destination": "orders:' || seq || '",'
					'"asset": "USD",'
					'"amount": 100'
				'},'
				'{'
					'"destination": "fees",'
					'"source": "orders:' || seq || '",'
					'"asset": "USD",'
					'"amount": 1'
				'},'
				'{'
					'"destination": "sellers:' || (seq % 5) || '",'
					'"source": "orders:' || seq || '",'
					'"asset": "USD",'
					'"amount": 99'
				'}'
	        '],'
	        '"metadata": { "tax": "1%" }'
		'},'
		'"accountMetadata": {'
			'"orders:' || seq || '": { "tax": "1%" }'
		'}'
	'}')::jsonb,
	'invalid-hash'
from generate_series(0, 100) as seq;

insert into logs(seq, ledger, id, type, date, data, hash)
select
	seq,
	'ledger' || seq % 5,
	(seq/5) + (seq % 5),
	'REVERTED_TRANSACTION',
	now(),
	('{'
		'"transaction": {'
			'"id": ' || (seq/5) + (seq % 5) || ','
			'"timestamp": ' || to_json(now()::timestamp without time zone) || ','
			'"postings": ['
				'{'
					'"source": "sellers:' || (seq % 5) || '",'
					'"destination": "orders:' || (seq-100) || '",'
					'"asset": "USD",'
					'"amount": 99'
				'},'
				'{'
					'"source": "fees",'
					'"destination": "orders:' || (seq-100) || '",'
					'"asset": "USD",'
					'"amount": 1'
				'},'
				'{'
					'"destination": "world",'
					'"source": "orders:' || (seq-100) || '",'
					'"asset": "USD",'
					'"amount": 100'
				'},'
				'{'
					'"source": "sellers:' || (seq % 5) || '",'
		            '"destination": "world",'
		            '"asset": "SELL",'
		            '"amount": 1'
		        '}'
	        '],'
	        '"metadata": { "tax": "1%" }'
		'},'
		'"revertedTransactionID": "' || ((seq-100)/5) + ((seq-100) % 5) || '"'
	'}')::jsonb,
	'invalid-hash'
from generate_series(101, 110) as seq;