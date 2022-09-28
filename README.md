## Sql Query Monitor - sqlm

The purpose of sqlm is to provide a way to instrument your sql statements, making really easy to log queries or measure execution time without modifying your actual code.

*Attach hooks to any database/sql driver.*

---

## Install

```
go get bitbucket.org/orientswiss/sqlm
```
It requires Go version >= 1.14

---
## Usage

```
    import (
        "bitbucket.org/orientswiss/sqlm"
        "github.com/go-sql-driver/mysql"
        "github.com/qustavo/sqlhooks/v2"
    )


	sql.Register("mysqlm", sqlhooks.Wrap(&mysql.MySQLDriver{}, &sqlm.QHooks{
		RWHost:     app.flags.DatabaseSet.Addr,
		RWDatabase: app.flags.DatabaseSet.Name,
		RWUser:     app.flags.DatabaseSet.User,
		ROHost:     app.flags.ReadDbAddress,
		RODatabase: app.flags.ReadDbName,
		ROUser:     app.flags.ReadDbUser,
	}))

```
Now one can use `mysqlm` driver instead of `mysql`
---