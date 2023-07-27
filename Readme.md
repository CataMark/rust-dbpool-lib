# Library for database connection pool connection a async operations

## Features
- database connection pool
- possibility for encrypted db connections based on connection strings
- async queries execution with timeout and query cancellation db signal
- .xlsx/ .csv/ .txt/ .json upload/ download
- batch upload/ download
- data types mapping: sql <-> rust <-> ms excel
- tested on ms sql, postgresql and sqlite

## Dependecies
- tokio runtime
- deadpool
- serde
- openssl
- ...



## Installation requirements:

-   openssl
-   for MSSQL pool make sure the user has the following permission: GRANT ALTER ANY connection TO <user>
