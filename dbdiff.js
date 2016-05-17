var _ = require('underscore')
var util = require('util')
var dialects = require('./dialects')
var dedent = require('dedent')

class DbDiff {
  _log (sql, level) {
    this.sql.push({ sql, level })
  }

  _drop (sql) {
    this._log(sql, 3)
  }

  _warn (sql) {
    this._log(sql, 2)
  }

  _safe (sql) {
    this._log(sql, 1)
  }

  _comment (sql) {
    this._log(sql, 0)
  }

  _compareTables (table1, table2) {
    var tableName = this._fullName(table1)

    var columNames1 = this._columnNames(table1)
    var columNames2 = this._columnNames(table2)

    var diff1 = _.difference(columNames1, columNames2)
    var diff2 = _.difference(columNames2, columNames1)

    diff1.forEach((columnName) => {
      this._drop(`ALTER TABLE ${tableName} DROP COLUMN "${columnName}";`)
    })

    diff2.forEach((columnName) => {
      var col = table2.columns.find((column) => column.name === columnName)
      this._safe(`ALTER TABLE ${tableName} ADD COLUMN "${columnName}" ${this._columnDescription(col)};`)
    })

    var common = _.intersection(columNames1, columNames2)
    common.forEach((columnName) => {
      var col1 = table1.columns.find((column) => column.name === columnName)
      var col2 = table2.columns.find((column) => column.name === columnName)

      if (col1.type !== col2.type) {
        this._warn(dedent`
          -- Previous data type was ${col1.type}
          ALTER TABLE ${tableName} ALTER COLUMN "${columnName}" SET DATA TYPE ${col2.type};
        `)
      }
      if (col1.nullable !== col2.nullable) {
        if (col2.nullable) {
          this._safe(`ALTER TABLE ${tableName} ALTER COLUMN "${columnName}" DROP NOT NULL;`)
        } else {
          this._warn(`ALTER TABLE ${tableName} ALTER COLUMN "${columnName}" SET NOT NULL;`)
        }
      }
    })
  }

  _createIndex (table, index) {
    var tableName = this._fullName(table)
    var keys = index.keys.map((key) => `"${key}"`) .join(',')
    this._safe(`CREATE INDEX "${index.name}" ON ${tableName} USING ${index.type} (${keys});`)
  }

  _dropIndex (index) {
    this._safe(`DROP INDEX "${index.schema}"."${index.name}";`)
  }

  _compareIndexes (table1, table2) {
    var indexNames1 = this._indexNames(table1)
    var indexNames2 = this._indexNames(table2)

    var diff1 = _.difference(indexNames1, indexNames2)
    var diff2 = _.difference(indexNames2, indexNames1)

    if (diff1.length > 0) {
      diff1.forEach((indexName) => {
        var index = table1.indexes.find((index) => index.name === indexName)
        this._dropIndex(index)
      })
    }
    if (diff2.length > 0) {
      diff2.forEach((indexName) => {
        var index = table2.indexes.find((index) => index.name === indexName)
        this._createIndex(table2, index)
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
        this._comment(`-- Index "${index.schema}"."${index.name}" needs to be changed`)
        this._dropIndex(index)
        this._createIndex(table1, index)
      }
    })
  }

  _compareSequences (db1, db2) {
    var sequenceNames1 = this._sequenceNames(db1)
    var sequenceNames2 = this._sequenceNames(db2)

    var diff1 = _.difference(sequenceNames1, sequenceNames2)
    var diff2 = _.difference(sequenceNames2, sequenceNames1)

    diff1.forEach((sequenceName) => {
      this._safe(`DROP SEQUENCE ${sequenceName};`)
    })

    diff2.forEach((sequenceName) => {
      var sequence = db2.sequences.find((sequence) => sequenceName === this._fullName(sequence))
      this._safe(this._sequenceDescription(sequence))
    })

    var inter = _.intersection(sequenceNames1, sequenceNames2)
    inter.forEach((sequenceName) => {
      var sequence1 = db1.sequences.find((sequence) => sequenceName === this._fullName(sequence))
      var sequence2 = db2.sequences.find((sequence) => sequenceName === this._fullName(sequence))

      var desc1 = this._sequenceDescription(sequence1)
      var desc2 = this._sequenceDescription(sequence2)

      if (desc2 !== desc1) {
        this._safe(`DROP SEQUENCE ${sequenceName};`)
        this._safe(desc2)
      }
    })
  }

  _compareConstraints (table1, table2) {
    var tableName = this._fullName(table2)
    table2.constraints.forEach((constraint2) => {
      var constraint1 = table1 && table1.constraints.find((cons) => constraint2.name === cons.name)
      if (constraint1) {
        if (_.isEqual(constraint1, constraint2)) return
        this._safe(`ALTER TABLE ${tableName} DROP CONSTRAINT "${constraint2.name}";`)
        constraint1 = null
      }
      if (!constraint1) {
        var keys = constraint2.keys.map((s) => `"${s}"`).join(', ')
        var func = (table1 ? this._warn : this._safe).bind(this)
        if (constraint2.type === 'primary') {
          func(`ALTER TABLE ${tableName} ADD CONSTRAINT "${constraint2.name}" PRIMARY KEY (${keys});`)
        } else if (constraint2.type === 'unique') {
          func(`ALTER TABLE ${tableName} ADD CONSTRAINT "${constraint2.name}" UNIQUE (${keys});`)
        } else if (constraint2.type === 'foreign') {
          var foreignKeys = constraint2.foreign_keys.map((s) => `"${s}"`).join(', ')
          func(`ALTER TABLE ${tableName} ADD CONSTRAINT "${constraint2.name}" FOREIGN KEY (${keys}) REFERENCES "${constraint2.foreign_table}" (${foreignKeys});`)
        }
      }
    })
  }

  compareSchemas (db1, db2) {
    this._compareSequences(db1, db2)

    db1.tables.forEach((table) => {
      var t = this._findTable(db2, table)
      if (!t) {
        this._drop(`DROP TABLE ${this._fullName(table)};`)
      }
    })

    db2.tables.forEach((table) => {
      var t = this._findTable(db1, table)
      var tableName = this._fullName(table)
      if (!t) {
        var columns = table.columns.map((col) => {
          return `\n  "${col.name}" ${this._columnDescription(col)}`
        })
        this._safe(`CREATE TABLE ${tableName} (${columns.join(',')}\n);`)

        var indexNames2 = this._indexNames(table)
        indexNames2.forEach((indexName) => {
          var index = table.indexes.find((index) => index.name === indexName)
          this._safe(`CREATE INDEX "${index.name}" ON ${tableName} USING ${index.type} (${index.keys.join(', ')});`)
        })
      } else {
        this._compareTables(t, table)
        this._compareIndexes(t, table)
      }
    })

    db2.tables.forEach((table) => {
      var t = this._findTable(db1, table)
      this._compareConstraints(t, table)
    })
  }

  compare (conn1, conn2) {
    this.sql = []
    return Promise.all([
      dialects.describeDatabase(conn1),
      dialects.describeDatabase(conn2)
    ])
    .then((results) => {
      var db1 = results[0]
      var db2 = results[1]
      this.compareSchemas(db1, db2)
    })
  }

  _commentOut (sql) {
    return sql.split('\n').map((line) => line.substring(0, 2) === '--' ? line : `-- ${line}`).join('\n')
  }

  _columnNames (table) {
    return table.columns.map((col) => col.name).sort()
  }

  _columnDescription (col) {
    var desc = col.type
    if (col.defaultValue) {
      desc += ' DEFAULT ' + col.defaultValue
    }
    desc += col.nullable ? ' NULL' : ' NOT NULL'
    return desc
  }

  _indexNames (table) {
    return table.indexes.map((index) => index.name).sort()
  }

  _isNumber (n) {
    return +n == n // eslint-disable-line
  }

  _sequenceDescription (sequence) {
    return util.format('CREATE SEQUENCE %s INCREMENT %s %s %s %s %s CYCLE;',
      this._fullName(sequence),
      sequence.increment,
      this._isNumber(sequence.minimum_value) ? 'MINVALUE ' + sequence.minimum_value : 'NO MINVALUE',
      this._isNumber(sequence.maximum_value) ? 'MAXVALUE ' + sequence.maximum_value : 'NO MAXVALUE',
      this._isNumber(sequence.start_value) ? 'START ' + sequence.start_value : '',
      sequence.cycle ? '' : 'NO'
    )
  }

  _sequenceNames (db) {
    return db.sequences.map((sequence) => this._fullName(sequence))
  }

  _fullName (obj) {
    return `"${obj.schema}"."${obj.name}"`
  }

  _findTable (db, table) {
    return db.tables.find((t) => t.name === table.name && t.schema === table.schema)
  }

  commands (type) {
    var level = 1
    if (type === 'drop') level = 3
    else if (type === 'warn') level = 2
    return this.sql.map((sql) => {
      return sql.level > level
        ? this._commentOut(sql.sql)
        : sql.sql
    }).join('\n\n')
  }
}

module.exports = DbDiff
