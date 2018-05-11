var dialects = require('./')
var pync = require('pync')
var MysqlClient = require('./mysql-client')

class MySQLDialect {
  _quote (str) {
    return '`' + str + '`'
  }
  _singleQuote (str) {
    return !str ? str : '\'' + str + '\'';
  }
  describeDatabase (options) {
    var schema = { dialect: 'mysql', sequences: [] }
    var client = new MysqlClient(options)
    return client.query('SHOW TABLES')
      .then((result) => {
        var field = result.fields[0].name
        var rows = result.rows
        var tables = rows.map((row) => row[field])

        return pync.map(tables, (table) => {
          var t = {
            name: table,
            constraints: [],
            indexes: []
          }
          return client.find(`DESCRIBE ${this._quote(table)}`)
            .then((columns) => {
              t.columns = columns.map((column) => ({
                name: column.Field,
                nullable: column.Null === 'YES',
                default_value: this._singleQuote(column.Default),
                type: column.Type,
                extra: column.Extra
              }))
              return t
            })
        })
      })
      .then((tables) => {
        schema.tables = tables
        return client.find('SELECT * FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=?', [client.database])
      })
      .then((constraints) => {
        constraints.forEach((constraint) => {
          var name = constraint['CONSTRAINT_NAME']
          var table = schema.tables.find((table) => table.name === constraint['TABLE_NAME'])
          var info = table.constraints.find((constr) => constr.name === name)
          var foreign = !!constraint['REFERENCED_TABLE_NAME']
          if (!info) {
            info = {
              name,
              type: foreign ? 'foreign' : (name === 'PRIMARY' ? 'primary' : 'unique'),
              columns: []
            }
            if (foreign) info.referenced_columns = []
            table.constraints.push(info)
          }
          if (foreign) {
            info.referenced_table = constraint['REFERENCED_TABLE_NAME']
            info.referenced_columns.push(constraint['REFERENCED_COLUMN_NAME'])
          }
          info.columns.push(constraint['COLUMN_NAME'])
        })
        return pync.series(schema.tables, (table) => (
          client.find(`SHOW INDEXES IN ${this._quote(table.name)}`)
            .then((indexes) => {
              indexes
                .filter((index) => !table.constraints.find((constraint) => constraint.name === index.Key_name))
                .forEach((index) => {
                  var info = table.indexes.find((indx) => index.Key_name === indx.name)
                  if (!info) {
                    info = {
                      name: index.Key_name,
                      type: index.Index_type,
                      columns: []
                    }
                    table.indexes.push(info)
                  }
                  info.columns.push(index.Column_name)
                })
            })
        ))
      })
      .then(() => schema)
  }
}

dialects.register('mysql', MySQLDialect)
