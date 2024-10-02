# Migrations test

This package allow to test the migration of an existing database regarding current code.

The test can be run using the following command : 
```shell
go test . \
  -databases.source <database source dsn>
```

The test will start a new postgres server, copy the database inside, then apply migrations.

Additionally, you can add the flag : 
```shell
go test . \
  -databases.source <database source dsn> \
  -databases.destination <database destination dsn>
```

In this case, the destination database will be used and no local postgres server will be started.
