#!/usr/bin/env node

var _ = require('underscore')
var Client = require('./client')
var util = require('util')
var pync = require('pync')

var dbdiff = module.exports = {}

dbdiff.log = function () {
  var msg = util.format.apply(null, Array.prototype.slice.call(arguments))
  dbdiff.logger(msg)
}

dbdiff.describeDatabase = (conString) => {
  var client = new Client(conString)
  var schema = {}

  return client.find('SELECT * FROM pg_tables WHERE schemaname NOT IN ($1, $2, $3)', ['temp', 'pg_catalog', 'information_schema'])
    .then((tables) => (
      pync.map(tables, (table) => {
        var t = {
          name: table.tablename,
          schema: table.schemaname,
          indexes: []
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
            defaultValue: column.column_default,
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
          AND ns.nspname NOT IN ('pg_catalog', 'pg_toast');
      `)
    })
    .then((indexes) => {
      indexes.forEach((index) => {
        var table = schema.tables.find((table) => table.name === index.indrelid && table.schema === index.nspname)
        table.indexes.push({
          name: index.indname,
          schema: table.schema,
          primary: index.indisprimary,
          unique: index.indisunique,
          type: index.indam,
          keys: index.indkey_names
        })
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
      client.end()
      return schema
    })
    .catch((err) => {
      client.end()
      return Promise.reject(err)
    })
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

function columnNames (table) {
  return table.columns.map((col) => col.name).sort()
}

function columnDescription (col) {
  var desc = col.type
  if (col.defaultValue) {
    desc += ' DEFAULT ' + col.defaultValue
  }
  desc += col.nullable ? ' NULL' : ' NOT NULL'
  return desc
}

function compareTables (table1, table2) {
  var tableName = fullName(table1)

  var columNames1 = columnNames(table1)
  var columNames2 = columnNames(table2)

  var diff1 = _.difference(columNames1, columNames2)
  var diff2 = _.difference(columNames2, columNames1)

  diff1.forEach((columnName) => {
    dbdiff.log(`ALTER TABLE ${tableName} DROP COLUMN "${columnName}";`)
    dbdiff.log()
  })

  diff2.forEach((columnName) => {
    var col = table2.columns.find((column) => column.name === columnName)
    dbdiff.log('ALTER TABLE %s ADD COLUMN "%s" %s;', tableName, columnName, columnDescription(col))
    dbdiff.log()
  })

  var common = _.intersection(columNames1, columNames2)
  common.forEach((columnName) => {
    var col1 = table1.columns.find((column) => column.name === columnName)
    var col2 = table2.columns.find((column) => column.name === columnName)

    if (col1.type !== col2.type) {
      dbdiff.log('-- Previous data type was %s', col1.type)
      dbdiff.log('ALTER TABLE %s ALTER COLUMN "%s" SET DATA TYPE %s;', tableName, columnName, col2.type)
      dbdiff.log()
    }
    if (col1.nullable !== col2.nullable) {
      if (col2.nullable) {
        dbdiff.log('ALTER TABLE %s ALTER COLUMN "%s" DROP NOT NULL;', tableName, columnName)
      } else {
        dbdiff.log('ALTER TABLE %s ALTER COLUMN "%s" SET NOT NULL;', tableName, columnName)
      }
      dbdiff.log()
    }
  })
}

function indexNames (table) {
  return table.indexes.map((index) => index.name).sort()
}

function dropIndex (index) {
  dbdiff.log('DROP INDEX "%s"."%s";', index.schema, index.name)
}

function createIndex (table, index) {
  var tableName = fullName(table)
  if (index.primary) {
    dbdiff.log('ALTER TABLE %s ADD CONSTRAINT "%s" PRIMARY KEY (%s);', tableName, index.name, index.keys.join(','))
  } else if (index.unique) {
    dbdiff.log('ALTER TABLE %s ADD CONSTRAINT "%s" UNIQUE (%s);', tableName, index.name, index.keys.join(','))
  } else {
    dbdiff.log('CREATE INDEX "%s" ON %s USING %s (%s);', index.name, tableName, index.type, index.keys.join(','))
  }
}

function compareIndexes (table1, table2) {
  var indexNames1 = indexNames(table1)
  var indexNames2 = indexNames(table2)

  var diff1 = _.difference(indexNames1, indexNames2)
  var diff2 = _.difference(indexNames2, indexNames1)

  if (diff1.length > 0) {
    diff1.forEach((indexName) => {
      var index = table1.indexes.find((index) => index.name === indexName)
      dropIndex(index)
    })
  }
  if (diff2.length > 0) {
    diff2.forEach((indexName) => {
      var index = table2.indexes.find((index) => index.name === indexName)
      createIndex(table2, index)
    })
  }

  var inter = _.intersection(indexNames1, indexNames2)
  inter.forEach((indexName) => {
    var index1 = table1.indexes.find((index) => index.name === indexName)
    var index2 = table2.indexes.find((index) => index.name === indexName)

    if (_.difference(index1.keys, index2.keys).length > 0 ||
      index1.primary !== index2.primary ||
      index1.unique !== index2.unique) {
      var index = index2
      dbdiff.log('-- Index "%s"."%s" needs to be changed', index.schema, index.name)
      dropIndex(index)
      createIndex(table1, index)
      dbdiff.log()
    }
  })
}

function isNumber (n) {
  return +n == n // eslint-disable-line
}

function sequenceDescription (sequence) {
  return util.format('CREATE SEQUENCE %s INCREMENT %s %s %s %s %s CYCLE;',
      fullName(sequence),
      sequence.increment,
      isNumber(sequence.minimum_value) ? 'MINVALUE ' + sequence.minimum_value : 'NO MINVALUE',
      isNumber(sequence.maximum_value) ? 'MAXVALUE ' + sequence.maximum_value : 'NO MAXVALUE',
      isNumber(sequence.start_value) ? 'START ' + sequence.start_value : '',
      sequence.cycle ? '' : 'NO'
    )
}

function sequenceNames (db) {
  return db.sequences.map(fullName).sort()
}

function compareSequences (db1, db2) {
  var sequenceNames1 = sequenceNames(db1)
  var sequenceNames2 = sequenceNames(db2)

  var diff1 = _.difference(sequenceNames1, sequenceNames2)
  var diff2 = _.difference(sequenceNames2, sequenceNames1)

  diff1.forEach((sequenceName) => {
    dbdiff.log('DROP SEQUENCE %s;', sequenceName)
    dbdiff.log()
  })

  diff2.forEach((sequenceName) => {
    var sequence = db2.sequences.find((sequence) => sequenceName === fullName(sequence))
    dbdiff.log(sequenceDescription(sequence))
    dbdiff.log()
  })

  var inter = _.intersection(sequenceNames1, sequenceNames2)
  inter.forEach((sequenceName) => {
    var sequence1 = db1.sequences.find((sequence) => sequenceName === fullName(sequence))
    var sequence2 = db2.sequences.find((sequence) => sequenceName === fullName(sequence))

    var desc1 = sequenceDescription(sequence1)
    var desc2 = sequenceDescription(sequence2)

    if (desc2 !== desc1) {
      dbdiff.log('DROP SEQUENCE %s;', sequenceName)
      dbdiff.log(desc2)
      dbdiff.log()
    }
  })
}

function fullName (obj) {
  return `"${obj.schema}"."${obj.name}"`
}

function findTable (db, table) {
  return db.tables.find((t) => t.name === table.name && t.schema === table.schema)
}

dbdiff.compareSchemas = function (db1, db2) {
  compareSequences(db1, db2)

  db1.tables.forEach((table) => {
    var t = findTable(db2, table)
    if (!t) {
      dbdiff.log(`DROP TABLE ${fullName(table)};`)
      dbdiff.log()
    }
  })

  db2.tables.forEach((table) => {
    var t = findTable(db1, table)
    var tableName = fullName(table)
    if (!t) {
      var columns = table.columns.map((col) => {
        return `\n  "${col.name}" ${columnDescription(col)}`
      })
      dbdiff.log(`CREATE TABLE ${tableName} (${columns.join(',')}\n);`)
      dbdiff.log()

      var indexNames2 = indexNames(table)
      indexNames2.forEach((indexName) => {
        var index = table.indexes.find((index) => index.name === indexName)
        dbdiff.log(`CREATE INDEX "${index.name}" ON ${tableName} USING ${index.type} (${index.keys.join(', ')});`)
        dbdiff.log()
      })
    } else {
      compareTables(t, table)
      compareIndexes(t, table)
    }
  })
}

dbdiff.compareDatabases = (conn1, conn2, callback) => {
  return Promise.all([
    dbdiff.describeDatabase(conn1),
    dbdiff.describeDatabase(conn2)
  ])
  .then((results) => {
    var db1 = results[0]
    var db2 = results[1]
    dbdiff.compareSchemas(db1, db2)
  })
}

if (module.id === require.main.id) {
  var yargs = require('yargs')
  var argv = yargs
      .usage('Usage: $0 conn_string1 conn_string2')
      .example('$0 postgres://user:pass@host[:port]/dbname1 postgres://user:pass@host[:port]/dbname2',
        'compares the scheme of two databases and prints the SQL commands to modify the first one in order to match the second one')
      .demand(2)
      .wrap(yargs.terminalWidth())
      .help('h')
      .alias('h', 'help')
      .argv

  var conn1 = argv._[0]
  var conn2 = argv._[1]
  dbdiff.logger = (msg) => {
    console.log(msg)
  }
  dbdiff.compareDatabases(conn1, conn2)
    .then(() => process.exit(0))
    .catch((err) => {
      console.error(err.stack)
      process.exit(1)
    })
}
