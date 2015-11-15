var pg = require('pg')
var txain = require('txain')
var dbdiff = require('../')
var assert = require('assert')

var conString1 = 'postgres://postgres:postgres@localhost/db1'
var conString2 = 'postgres://postgres:postgres@localhost/db2'

var client1, client2

exports.connect = function(callback) {
  if (client1) return callback()

  client1 = new pg.Client(conString1)
  client2 = new pg.Client(conString2)

  var arr = [client1, client2]
  txain(arr)
  .each(function(client, callback) {
    client.connect(callback)
  })
  .end(callback)
}

exports.resetDatabases = function(callback) {
  txain(function(callback) {
    exports.connect(callback)
  })
  .then(function(callback) {
    callback(null, [client1, client2])
  })
  .each(function(client, callback) {
    client.query('drop schema public cascade; create schema public;', callback)
  })
  .end(callback)
}

exports.runCommands = function(commands1, commands2, callback) {
  txain(function(callback) {
    callback(null, commands1)
  })
  .each(function(command, callback) {
    client1.query(command, callback)
  })
  .then(function(callback) {
    callback(null, commands2)
  }).each(function(command, callback) {
    client2.query(command, callback)
  })
  .end(callback)
}

exports.runAndCompare = function(commands1, commands2, expected, callback) {
  var arr = []

  dbdiff.logger = function(msg) {
    if (msg) {
      arr.push(msg)
    }
  }

  txain(function(callback) {
    exports.runCommands(commands1, commands2, callback)
  })
  .then(function(callback) {
    dbdiff.compareDatabases(conString1, conString2, callback)
  })
  .then(function(callback) {
    // run the expected commands
    client1.query(arr.join('\n'), callback)
  })
  .then(function(callback) {
    assert.deepEqual(arr, expected)
    // compare again the dbs
    arr.splice(0)
    dbdiff.compareDatabases(conString1, conString2, callback)
  })
  .then(function(callback) {
    assert.deepEqual(arr, [])
    callback(null, arr)
  })
  .end(callback)
}
