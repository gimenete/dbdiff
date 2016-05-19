var pg = require('pg')
var querystring = require('querystring')

class PostgresClient {
  constructor (options) {
    var conString
    if (typeof options === 'string') {
      conString = options
    } else {
      var dialectOptions = Object.assign({}, options.dialectOptions)
      Object.keys(dialectOptions).forEach((key) => {
        var value = dialectOptions[key]
        if (typeof value === 'boolean') {
          dialectOptions[key] = value ? 'true' : 'false'
        }
      })
      var query = querystring.stringify(dialectOptions)
      if (query.length > 0) query = '?' + query
      conString = `postgres://${options.username}:${options.password}@${options.host}:${options.port || 5432}/${options.database}${query}`
    }
    this.conString = conString
  }

  dropTables () {
    return this.query('drop schema public cascade; create schema public;')
  }

  connect () {
    return new Promise((resolve, reject) => {
      if (this.client) return resolve()
      pg.connect(this.conString, (err, client, done) => {
        if (err) return reject(err)
        this.client = client
        this.done = done
        resolve()
      })
    })
  }

  query (sql, params = []) {
    return this.connect()
      .then(() => {
        return new Promise((resolve, reject) => {
          this.client.query(sql, params, (err, result) => {
            this.done()
            err ? reject(err) : resolve(result)
          })
        })
      })
  }

  find (sql, params = []) {
    return this.query(sql, params).then((result) => result.rows)
  }

  findOne (sql, params = []) {
    return this.query(sql, params).then((result) => result.rows[0])
  }
}

module.exports = PostgresClient
