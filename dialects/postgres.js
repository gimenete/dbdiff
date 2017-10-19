var dialects = require('./')
var pync = require('pync')
var PostgresClient = require('./postgres-client')

class PostgresDialect {
  _unquote (str) {
    if (str.substring(0, 1) === '"' && str.substring(str.length - 1) === '"') {
      return str.substring(1, str.length - 1)
    }
    return str
  }

  describeDatabase (options) {
    var schema = { dialect: 'postgres' }
    var client = new PostgresClient(options)
    return client.find('SELECT * FROM pg_tables WHERE schemaname NOT IN ($1, $2, $3)', ['temp', 'pg_catalog', 'information_schema'])
      .then((tables) => (
        pync.map(tables, (table) => {
          var t = {
            name: table.tablename,
            schema: table.schemaname,
            indexes: [],
            constraints: []
          }
          return client.find(`
            SELECT
              table_name,
              table_schema,
              column_name,
              data_type,
              udt_name,
              character_maximum_length,
              is_nullable,
              column_default
            FROM
              INFORMATION_SCHEMA.COLUMNS
            WHERE
              table_name=$1 AND table_schema=$2;`, [table.tablename, table.schemaname])
          .then((columns) => {
            t.columns = columns.map((column) => ({
              name: column.column_name,
              nullable: column.is_nullable === 'YES',
              default_value: column.column_default,
              type: dataType(column)
            }))
            return t
          })
        })
      ))
      .then((tables) => {
        schema.tables = tables
        return client.find(`
          SELECT
            i.relname as indname,
            i.relowner as indowner,
            idx.indrelid::regclass,
            idx.indisprimary,
            idx.indisunique,
            am.amname as indam,
            idx.indkey,
            ARRAY(
              SELECT pg_get_indexdef(idx.indexrelid, k + 1, true)
              FROM generate_subscripts(idx.indkey, 1) as k
              ORDER BY k
            ) AS indkey_names,
            idx.indexprs IS NOT NULL as indexprs,
            idx.indpred IS NOT NULL as indpred,
            ns.nspname
          FROM
            pg_index as idx
          JOIN pg_class as i
            ON i.oid = idx.indexrelid
          JOIN pg_am as am
            ON i.relam = am.oid
          JOIN pg_namespace as ns
            ON ns.oid = i.relnamespace
            AND ns.nspname NOT IN ('pg_catalog', 'pg_toast')
          WHERE (NOT idx.indisprimary) AND (NOT idx.indisunique);
        `)
      })
      .then((indexes) => {
        indexes.forEach((index) => {
          var tableName = index.indrelid.split('.').pop()
          var table = schema.tables.find((table) => table.name === tableName && table.schema === index.nspname)
          table.indexes.push({
            name: index.indname,
            schema: table.schema,
            type: index.indam,
            columns: index.indkey_names
          })
        })
        return client.find(`
          SELECT conrelid::regclass AS table_from, n.nspname, contype, conname, pg_get_constraintdef(c.oid) AS description
          FROM   pg_constraint c
          JOIN   pg_namespace n ON n.oid = c.connamespace
          WHERE  contype IN ('f', 'p', 'u')
          ORDER  BY conrelid::regclass::text, contype DESC;
        `)
      })
      .then((constraints) => {
        var types = {
          u: 'unique',
          f: 'foreign',
          p: 'primary'
        }
        constraints.forEach((constraint) => {
          var tableFrom = this._unquote(constraint.table_from).split('.').pop()
          var table = schema.tables.find((table) => table.name === tableFrom && table.schema === constraint.nspname)
          var { description } = constraint
          var i = description.indexOf('(')
          var n = description.indexOf(')')
          var m = description.indexOf('REFERENCES')
          var info = {
            name: constraint.conname,
            schema: table.schema,
            type: types[constraint.contype],
            columns: description.substring(i + 1, n).split(',').map((s) => this._unquote(s.trim()))
          }
          table.constraints.push(info)
          if (m > 0) {
            var substr = description.substring(m + 'REFERENCES'.length)
            i = substr.indexOf('(')
            n = substr.indexOf(')')
            info.referenced_table = substr.substring(0, i).trim()
            info.referenced_columns = substr.substring(i + 1, n).split(',').map((s) => this._unquote(s.trim()))
          }
        })
        return client.find('SELECT * FROM information_schema.sequences')
      })
      .then((sequences) => {
        schema.sequences = sequences.map((sequence) => {
          sequence.schema = sequence.sequence_schema
          sequence.name = sequence.sequence_name
          sequence.cycle = sequence.cycle_option === 'YES'
          delete sequence.sequence_name
          delete sequence.sequence_catalog
          delete sequence.sequence_schema
          delete sequence.cycle_option
          return sequence
        })
        return schema
      })
  }
}

function dataType (info) {
  var type
  if (info.data_type === 'ARRAY') {
    type = info.udt_name
    if (type.substring(0, 1) === '_') {
      type = type.substring(1)
    }
    type += '[]'
  } else if (info.data_type === 'USER-DEFINED') {
    type = info.udt_name // hstore for example
  } else {
    type = info.data_type
  }

  if (info.character_maximum_length) {
    type = type + '(' + info.character_maximum_length + ')'
  }
  return type
}

dialects.register('postgres', PostgresDialect)
