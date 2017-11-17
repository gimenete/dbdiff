var mysql = require('mysql')
var url = require('url')
var querystring = require('querystring')

var connections = {};

class MysqlClient {
  constructor (options) {
    if (typeof options === 'string') {
      var info = url.parse(options)
      var auth = info.auth && info.auth.split(':')
      var more = info.query && querystring.parse(info.query)
      // get port from url parser, input options, or default to 3306
      var port = info.port ? info.port : options.port ? options.port : '3306'
      options = Object.assign({
        dialect: 'mysql',
        username: auth[0],
        password: auth[1],
        database: (info.pathname || '/').substring(1),
        host: info.hostname // host is 'localhost:port' hostname is just 'localhost'
      }, more)
    }

    this.options = Object.assign({
      user: options.username,
      port: port,
      multipleStatements: true
    }, options)
    this.database = options.database

    var key = `${options.username}:${options.password}@${options.host}:${port}/${options.database}`
    var conn = connections[key]

    if (!conn) {
      conn = connections[key] = mysql.createConnection(this.options)
    }
    this.connection = conn
  }

  dropTables() {
    const sql = `
        SELECT concat('DROP TABLE IF EXISTS ', table_name, ';') AS fullSQL
        FROM information_schema.tables
        WHERE table_schema = ?;`;
    const params = [this.options.database];
    return this.query(sql, params)
      .then((results) => {
        results = results.rows
        var sql_result = results.map((result) => result.fullSQL).join(' ')
        return sql_result && this.query(sql_result)
      }).catch((reason) => {
            console.error('dropTables failed ('+JSON.stringify(reason)+').');
      });
  }

  /**
   * connect - create a database connection
   * resolve(threadId) -  returns threadId
   * reject(err) - returns connection error
   * @return {type}  Promise
   */
  connect() {
    return new Promise((resolve, reject) => {
      var conn = mysql.createConnection(this.options);
      this.connection = conn
      this.connection.connect((err) => {
        if (err) { return reject(err); }
        return resolve(this.connection.threadId);
      });
    });
  }


  /**
   * query - execute a sql query with the given parametrs
   *
   * @param  {type} sql         MySQL statment
   * @param  {type} params = [] query paramenters
   * @return {type}             Promise: resolve({ rows, fields }), reject(err)
   */
  _query(sql, params = []) {
    return new Promise((resolve, reject) => {
      this.connection.query(sql, params, (err, rows, fields) => {
        if (err) return reject(err);
        return resolve({ rows, fields });
      })
    })
  }

  find(sql, params = []) {
    return this.query(sql, params)
      .then((result) => {
        return result.rows
      }).catch((reason) => {
          console.error('find query failed ('+ JSON.stringify(reason)+').');
    });
  }

  findOne(sql, params = []) {
    // this is returning a Promise because this.query returns a promise
    return this.query(sql, params)
      .this((result) => {
        return result.rows[0];
      }).catch((error) => {
        console.error('findOne query faild - ('+reason+').');
      });
  }

  /**
   * query - executes a query and closes the connection
   * resolve() is called when the query is executed (even with a connection close exception)
   * reject() is called when the query can not be executed
   * If there is a (connection error or a query error) and a close error, the close error is appended to the first error's supressed Array
   * If there is a valid result and a close error, the close error is appended to the results supressed Array
   * Result {"rows": {}, "fields": {}}
   * @param  {type} sql         sql statment
   * @param  {type} params = [] sql parameters
   * @return {type}             Promise
   */
  query(sql, params = []) {
    return new Promise((resolve, reject) => {
      this.connect().then((threadId) => {
        // connected to the database
        this._query(sql, params)
          .then((queryResult) => {
                // query returned a result, attempt to close the connection
                this.close(null, queryResult)
                  .then((result) => {
                    // connection was closed
                    resolve(result)
                  }).catch((res) => {
                    // could not close the connection
                    if(res.supressed) console.error('db close error. ' + res.supressed);
                    resolve(res)
                  });
            }).catch((error) => {
                // there was an error with the query, attempt to close the connection
                this.close(error, null)
                  .then((err) => {
                    // connection was closed
                    return reject(err);
                  }).catch((e) => {
                    if(e.supressed) console.error('db close error. ' + e.supressed);
                    // could not close the connection
                    reject(e);
                  })
            });
      }).catch((connError) => {
        // could not connect to the database // might not need to attempt to close if you don't have a valid connection
        this.close(connError, null)
          .then((error) => {
            // connection was closed
            return reject(error);
          }).catch((e) => {
            // could not close the connection
            reject(e);
          });
      });
    });
  }

  /**
   * close - close database connection
   * If the connection is closed without errors, resolve() is called
   * If the connection can not be closed, reject() is called
   * If the connection can't be closed and a queryError or queryResult is provided, the close arrow is appended to the object as obj.supressed Array
   * @param  {type} queryError  error returned from a query or null
   * @param  {type} queryResult result returned from a query or null
   * @return {type}             Promise
   */
  close(queryError , queryResult) {
    return new Promise((resolve, reject) => {
      this.connection.end((dbError) => {
        if(dbError && queryResult) { return reject(this._appendError(queryResult, dbError));} // db close error and query result. supress db error
        if(dbError && queryError) {  return reject(this._appendError(queryError, dbError)); } // db error and query error. supress db error
        if(dbError) { return reject(dbErr); } // only db error
        if(queryError) { return resolve(queryError); } // only query error (resolve because db close was a success)
        if(queryError === null && queryResult === null) { return resolve(); } // normal call to close() without a prior query
        return resolve(queryResult); // pass up the query result
        // its impossible to get a (queryError && queryResult)
      });
    });
  }



  /**
   * _appendError - appends an error onto an object as a .supressed attribute.
   * Will not overwrite an existing .supressed attribute
   *
   * @param  {type} object description
   * @param  {type} error  description
   * @return {type}        description
   */
  _appendError(object, error){
    let errors = [];
    for (var prop in object) {
      if(prop === 'supressed') {
        if(Array.isArray(object[prop])){
          var clone = object[prop].slice(0);
          clone.push(error);
          errors = clone;
        }
        else {
          throw new Error('can not add to supressed. it already exsists and is not an array');
        }
      }
    }
    if (errors.length <= 0) errors = new Array(error);
    // add supressed error as 1st object in array
    object = Object.assign({supressed: errors}, object);
    return object
  }
}

module.exports = MysqlClient
