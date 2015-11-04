#!/usr/bin/env node

var txain = require('txain')
var multiline = require('multiline')
var _ = require('underscore')
var pg = require('pg')
var util = require('util')

var dbdiff = module.exports = {}

dbdiff.describeDatabase = function(conString, callback) {
  var client = new pg.Client(conString)
  var schema = {
    tables: {},
  }

  txain(function(callback) {
    client.connect(callback)
  })
  .then(function(client, done, callback) {
    client.query('SELECT * FROM pg_tables WHERE schemaname NOT IN ($1, $2, $3)', ['temp', 'pg_catalog', 'information_schema'], callback)
  })
  .then(function(result, callback) {
    callback(null, result.rows)
  })
  .map(function(table, callback) {
    var query = multiline(function() {/*
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
        table_name=$1 AND table_schema=$2;
    */})
    client.query(query, [table.tablename, table.schemaname], callback)
  })
  .then(function(descriptions, callback) {
    var tables = schema.tables = {}
    descriptions.forEach(function(desc) {
      desc.rows.forEach(function(row) {
        var tableName = util.format('"%s"."%s"', row.table_schema, row.table_name)
        var table = tables[tableName]
        if (!table) {
          tables[tableName] = []
          table = tables[tableName]
        }
        delete row.table_schema
        delete row.table_name
        table.push(row)
      })
    })

    var query = multiline(function() {/*
      SELECT
        i.relname as indname,
        i.relowner as indowner,
        idx.indrelid::regclass,
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
    */})
    client.query(query, callback)
  })
  .then(function(result, callback) {
    schema.indexes = result.rows
    client.query('SELECT * FROM information_schema.sequences', callback)
  }).then(function(result, callback) {
    schema.sequences = result.rows
    schema.sequences.forEach(function(sequence) {
      sequence.name = util.format('"%s"."%s"', sequence.sequence_schema, sequence.sequence_name)
    })
    client.query('SELECT current_schema()', callback)
  })
  .end(function(err, result) {
    client.end()
    if (err) return callback(err)
    schema.public_schema = result.rows[0].current_schema
    callback(null, schema)
  })
}

function dataType(info) {
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
    type = type+'('+info.character_maximum_length+')'
  }
  return type
}

function columnNames(columns) {
  return columns.map(function(col) {
    return col.column_name
  }).sort()
}

function columnDescription(col) {
  var desc = dataType(col)
  if (col.column_default) {
    desc += ' DEFAULT '+col.column_default
  }
  desc += col.is_nullable === 'NO' ? ' NOT NULL' : ' NULL'
  return desc
}

function compareTables(tableName, db1, db2) {
  var table1 = db1.tables[tableName]
  var table2 = db2.tables[tableName]

  var columNames1 = columnNames(table1)
  var columNames2 = columnNames(table2)

  var diff1 = _.difference(columNames1, columNames2)
  var diff2 = _.difference(columNames2, columNames1)
  
  diff1.forEach(function(columnName) {
    console.log('ALTER TABLE %s DROP COLUMN "%s";', tableName, columnName)
    console.log()
  })

  diff2.forEach(function(columnName) {
    var col = _.findWhere(table2, { column_name: columnName })
    var type = dataType(col)
    console.log('ALTER TABLE %s ADD COLUMN "%s" %s;', tableName, columnName, columnDescription(col))
    console.log()
  })

  var common = _.intersection(columNames1, columNames2)
  common.forEach(function(columnName) {
    var col1 = _.findWhere(table1, { column_name: columnName })
    var col2 = _.findWhere(table2, { column_name: columnName })

    if (col1.data_type !== col2.data_type
      || col1.udt_name !== col2.udt_name
      || col1.character_maximum_length !== col2.character_maximum_length) {
      console.log('-- Previous data type was %s', dataType(col1))
      console.log('ALTER TABLE %s ALTER COLUMN "%s" SET DATA TYPE %s;', tableName, columnName, dataType(col2))
      console.log()
    }
    if (col1.is_nullable !== col2.is_nullable) {
      if (col2.is_nullable === 'YES') {
        console.log('ALTER TABLE %s ALTER COLUMN "%s" DROP NOT NULL;', tableName, columnName)
      } else {
        console.log('ALTER TABLE %s ALTER COLUMN "%s" SET NOT NULL;', tableName, columnName)
      }
      console.log()
    }
  })
}

function indexNames(tableName, indexes) {
  return _.filter(indexes, function(index) {
    return util.format('"%s".%s', index.nspname, index.indrelid) === tableName
  }).map(function(index) {
    return index.indname
  }).sort()
}

function compareIndexes(tableName, db1, db2) {
  var indexes1 = db1.indexes
  var indexes2 = db2.indexes

  var indexNames1 = indexNames(tableName, indexes1)
  var indexNames2 = indexNames(tableName, indexes2)

  var diff1 = _.difference(indexNames1, indexNames2)
  var diff2 = _.difference(indexNames2, indexNames1)

  if (diff1.length > 0) {
    diff1.forEach(function(indexName) {
      var index = _.findWhere(indexes1, { indname: indexName })
      console.log('DROP INDEX %s."%s";', index.nspname, indexName)
    })
  }
  if (diff2.length > 0) {
    diff2.forEach(function(indexName) {
      var index = _.findWhere(indexes2, { indname: indexName })
      console.log('CREATE INDEX "%s" ON %s USING %s (%s);', indexName, index.indrelid, index.indam, index.indkey_names.join(','))
    })
  }

  var inter = _.intersection(indexNames1, indexNames2)
  inter.forEach(function(indexName) {
    var index1 = _.findWhere(indexes1, { indname: indexName })
    var index2 = _.findWhere(indexes2, { indname: indexName })

    if (_.difference(index1.indkey_names, index2.indkey_names).length > 0) {
      var index = index2
      console.log('-- Index %s needs to be changed', index.indname)
      console.log('DROP INDEX %s."%s";', index.nspname, index.indname)
      console.log('CREATE INDEX "%s" ON %s USING %s (%s);', index.indname, index.indrelid, index.indam, index.indkey_names.join(','))
      console.log()
    }
  })
}

function isNumber(n) {
  return +n == n
}

function sequenceDescription(sequence) {
  return util.format('CREATE SEQUENCE %s INCREMENT %s %s %s %s %s CYCLE;',
      sequence.name,
      sequence.increment,
      isNumber(sequence.minimum_value) ? 'MINVALUE '+sequence.minimum_value : 'NO MINVALUE',
      isNumber(sequence.maximum_value) ? 'MAXVALUE '+sequence.maximum_value : 'NO MAXVALUE',
      isNumber(sequence.start_value) ? 'START '+sequence.start_value : '',
      sequence.cycle_option === 'NO' ? 'NO' : ''
    )
}

function sequenceNames(db) {
  return db.sequences.map(function(sequence) {
    return sequence.name
  }).sort()
}

function compareSequences(db1, db2) {
  var sequenceNames1 = sequenceNames(db1)
  var sequenceNames2 = sequenceNames(db2)

  var diff1 = _.difference(sequenceNames1, sequenceNames2)
  var diff2 = _.difference(sequenceNames2, sequenceNames1)
  
  diff1.forEach(function(sequenceName) {
    console.log('DROP SEQUENCE %s;', sequenceName)
    console.log()
  })

  diff2.forEach(function(sequenceName) {
    var sequence = _.findWhere(db2.sequences, { name: sequenceName })
    console.log(sequenceDescription(sequence))
    console.log()
  })

  var inter = _.intersection(sequenceNames1, sequenceNames2)
  inter.forEach(function(sequenceName) {
    var sequence1 = _.findWhere(db1.sequences, { name: sequenceName })
    var sequence2 = _.findWhere(db2.sequences, { name: sequenceName })

    var desc1 = sequenceDescription(sequence1)
    var desc2 = sequenceDescription(sequence2)

    if (desc2 !== desc1) {
      console.log('DROP SEQUENCE %s;', sequenceName)
      console.log(desc2)
      console.log()
    }
  })
}

dbdiff.compareSchemas = function(db1, db2) {
  compareSequences(db1, db2)

  var tableNames1 = _.keys(db1.tables).sort()
  var tableNames2 = _.keys(db2.tables).sort()

  var diff1 = _.difference(tableNames1, tableNames2)
  var diff2 = _.difference(tableNames2, tableNames1)
  
  diff1.forEach(function(tableName) {
    console.log('DROP TABLE %s;', tableName)
    console.log()
  })

  diff2.forEach(function(tableName) {
    var columns = db2.tables[tableName].map(function(col) {
      var type = dataType(col)
      return '\n  "'+col.column_name+'" '+columnDescription(col)
    })
    console.log('CREATE TABLE %s (%s', tableName, columns.join(','))
    console.log(');')
    console.log()

    var indexNames2 = indexNames(tableName, db2.indexes)
    indexNames2.forEach(function(indexName) {
      var index = _.findWhere(indexes2, { indname: indexName })
      console.log('CREATE INDEX "%s" ON %s USING %s (%s);', index.indname, index.indrelid, index.indam, index.indkey_names.join(','))
      console.log()
    })
  })

  var inter = _.intersection(tableNames1, tableNames2)
  inter.forEach(function(tableName) {
    compareTables(tableName, db1, db2)
    compareIndexes(tableName, db1, db2)
  })
}

dbdiff.compareDatabases = function(conn1, conn2) {
  var db1, db2
  txain(function(callback) {
    dbdiff.describeDatabase(conn1, callback)
  })
  .then(function(db, callback) {
    db1 = db
    dbdiff.describeDatabase(conn2, callback)
  })
  .then(function(db, callback) {
    db2 = db
    callback()
  })
  .end(function(err) {
    if (err) {
      console.error(String(err))
      process.exit(1)
      return
    }
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
  dbdiff.compareDatabases(conn1, conn2)
}
