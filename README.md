This is a small program that tests the MySQL performance of finding in what polygons a points falls into.

To install the MySQL driver using the [go tool](http://golang.org/cmd/go/ "go command") from shell:

```bash
$ go get github.com/go-sql-driver/mysql
```

To get this code:

```bash
$ go get github.com/razvanm/mysql-geotest
```

The schema used by the test is the following:

```sql
CREATE TABLE `geo` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `poly` polygon NOT NULL,
  `c` char(120) NOT NULL DEFAULT '',
  `pad` char(60) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  SPATIAL KEY `poly` (`poly`)
) ENGINE=MyISAM
```

How to create the table (10000 rows):

```bash
$ mysql-geotest prepare
```

How to run a test (1 thread, 1 minute):

```bash
$ mysql-geotest run
```

How to delete the table:

```bash
$ mysql-geotest run
```
