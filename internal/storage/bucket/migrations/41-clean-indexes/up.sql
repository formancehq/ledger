do $$
	begin
		set search_path = '{{ .Schema }}';

		drop index accounts_address_array;
		drop index accounts_address_array_length;
		drop index moves_ledger;
		drop index moves_range_dates;
		drop index moves_account_address;
		drop index moves_date;
		drop index moves_asset;
		drop index transactions_metadata_metadata;
		drop index accounts_metadata_metadata;
		drop index transactions_date;
		drop index transactions_metadata_index;
		drop index accounts_volumes_idx;
		drop index accounts_metadata_idx;
	end
$$;