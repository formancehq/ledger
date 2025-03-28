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
			'"timestamp": "' || now() || '",'
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