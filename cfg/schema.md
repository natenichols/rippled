## Schema 

This directory contains the schema definition file for the PostgreSQL 
database. It should be deployed prior to starting rippled.

The file **schema.sql** contains the main tables, indices, and constraints into 
which transactional data is inserted. There is a table for each transaction
(*transactions*) and a table one that maps each 
account to its transactions (*account_transactions*). This file should be 
deployed first against an empty database.


Deploy using the `psql` command like so:

```psql -f schema.sql postgres://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]```

Example:
```
psql -f schema.sql postgres://marktest3.rc.ripple.com/testing2
```

Example usage for inserting a transaction:

The field values as they correspond to SQLITE are:
```
SQLite       Postgres
LedgerSeq    ledger_seq
(none)       transaction_index
TransID      transaction_id
RawTxn       tx
TxnMeta      meta
```

I'm pretty sure the transaction_index value is in the transaction
object as sfTransactionIndex, but it's not currently inserted into SQLite.
This field is the index position for the transaction within the array
of transactions for the ledger represented by ledger_seq. Every transaction
is uniquely identified by ledger_seq and transaction_index. Also makes it
possible to sort them numerically.

PgQuery::querySync(pg_params) is the function to execute, something like as
follows:: 

```
pg_params cmd = {
    "INSERT INTO transactions
     (ledger_seq, transaction_index, transaction_id, tx, meta)
     VALUES ($1::bigint, $2::bigint, $3::bytea, $4::bytea, $5::bytea)
     ON CONFLICT DO NOTHING",
    {}};
cmd.second.push_back(std::to_string(LedgerSeq);
cmd.second.push_back(std::to_string(transaction_index);
cmd.second.push_back(std::to_string("\\x" + <hex transaction hash>);
cmd.second.push_back(std::to_string("\\x" + <hex transaction object>);
cmd.second.push_back(std::to_string("\\x" + <hex metadata object>);
auto res = PgQuery(app_.pgPool()).querySync(cmd);
```

PgFactory.cpp shows how to extract results. In this case, 1 tuple (aka row)
with 1 field containing the number of rows affected, either 0 or 1.

```
/* first: command
 * second: parameter values */
    using pg_params = std::pair<char const*,
        std::vector<std::optional<std::string>>>;
```
For AccountTransactions:

```
pg_params cmd = {
    "INSERT INTO account_transactions (account, ledger_seq, transaction_index)
     VALUES ($1::bytea, $2::bigint, $3::bigint)
     ON CONFLICT DO NOTHING",
    {}};
cmd.second.push_back(std::to_string("\\x" + <hex account address>);
cmd.second.push_back(std::to_string(ledger_seq);
cmd.second.push_back(std::to_string(transaction_index);
auto res = PgQuery(app_.pgPool()).querySync(cmd);
```
