# dbdiff
Compares two postgresql databases and prints SQL commands to modify the first one in order to match the second one

# Installing

Install globally with `npm`

```
npm install dbdiff -g
```

# Usage

```
dbdiff \
  postgres://user:pass@host[:port]/dbname1 \
  postgres://user:pass@host[:port]/dbname2
```

# Caveats

Some statements may fail or may produce data loss depending on the data stored in the target database. For example:

## Dropping tables and columns

`dbdiff` will generate `DROP TABLE` and `DROP COLUMN` statements. Make sure you want to drop those tables / columns.

## Changing the data type of existing columns

Postgresql is not able to change the existing data to the new data type. In that case you will get an error similar to this:

```
ERROR:  column "column_name" cannot be cast automatically to type integer
HINT:  Specify a USING expression to perform the conversion.
```

So you will need to specify a `USING` expression to perform de conversion. For example to convert text to integers:

```
ALTER TABLE table_name
  ALTER column_name TYPE data_type USING column_name::integer
```

## NOT NULL violations

If an existing column needs to be changed from nullable to not nullable the statement may fail if there are existing rows with a `NULL` value in that column.
In that case you will get an error like:

```
ERROR:  column "column_name" contains null values.
```

You should fill the existing rows with not null values before making the column not nullable.
