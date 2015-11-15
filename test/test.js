var dbdiff = require('../index')
var utils = require('./utils')
var assert = require('assert')
var exec = require('child_process').exec
var txain = require('txain')

describe('dbdiff.compareDatabases', function() {

  beforeEach(function(done) {
    utils.resetDatabases(done)
  })

  it('should create a table', function(done) {
    var commands1 = []
    var commands2 = ['CREATE TABLE users (email VARCHAR(255), tags varchar(255)[])']
    var expected = [
      'CREATE TABLE "public"."users" (\n  "email" character varying(255) NULL,\n  \"tags\" varchar[] NULL',
      ');',
    ]
    utils.runAndCompare(commands1, commands2, expected, done)
  })

  it('should drop a table', function(done) {
    var commands1 = ['CREATE TABLE users (email VARCHAR(255))']
    var commands2 = []
    var expected = [
      'DROP TABLE "public"."users";',
    ]
    utils.runAndCompare(commands1, commands2, expected, done)
  })

  it('should create a table wih an index', function(done) {
    var commands1 = []
    var commands2 = ['CREATE TABLE users (id serial)']
    var expected = [
      'CREATE SEQUENCE "public"."users_id_seq" INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 NO CYCLE;',
      'CREATE TABLE "public"."users" (\n  "id" integer DEFAULT nextval(\'users_id_seq\'::regclass) NOT NULL',
      ');',
    ]
    utils.runAndCompare(commands1, commands2, expected, done)
  })

  it('should add a column to a table', function(done) {
    var commands1 = ['CREATE TABLE users (email VARCHAR(255))']
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
    ]
    var expected = [
      'ALTER TABLE "public"."users" ADD COLUMN "first_name" character varying(255) NULL;',
    ]
    utils.runAndCompare(commands1, commands2, expected, done)
  })

  it('should drop a column from a table', function(done) {
    var commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
    ]
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
    ]
    var expected = [
      'ALTER TABLE "public"."users" DROP COLUMN "first_name";',
    ]
    utils.runAndCompare(commands1, commands2, expected, done)
  })

  it('should change the type of a column', function(done) {
    var commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(200)',
    ]
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
    ]
    var expected = [
      '-- Previous data type was character varying(200)',
      'ALTER TABLE "public"."users" ALTER COLUMN "first_name" SET DATA TYPE character varying(255);',
    ]
    utils.runAndCompare(commands1, commands2, expected, done)
  })

  it('should change a column to not nullable', function(done) {
    var commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
    ]
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255) NOT NULL',
    ]
    var expected = [
      'ALTER TABLE "public"."users" ALTER COLUMN "first_name" SET NOT NULL;',
    ]
    utils.runAndCompare(commands1, commands2, expected, done)
  })

  it('should change a column to nullable', function(done) {
    var commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255) NOT NULL',
    ]
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
    ]
    var expected = [
      'ALTER TABLE "public"."users" ALTER COLUMN "first_name" DROP NOT NULL;',
    ]
    utils.runAndCompare(commands1, commands2, expected, done)
  })

  it('should create a sequence', function(done) {
    var commands1 = []
    var commands2 = ['CREATE SEQUENCE seq_name']
    var expected = [
      'CREATE SEQUENCE "public"."seq_name" INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 NO CYCLE;',
    ]
    utils.runAndCompare(commands1, commands2, expected, done)
  })

  it('should drop a sequence', function(done) {
    var commands1 = ['CREATE SEQUENCE seq_name']
    var commands2 = []
    var expected = [
      'DROP SEQUENCE "public"."seq_name";',
    ]
    utils.runAndCompare(commands1, commands2, expected, done)
  })

  // TODO: update a sequence

  it('should create an index', function(done) {
    var commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
    ]
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
      'CREATE INDEX users_email ON users (email)',
    ]
    var expected = [
      'CREATE INDEX "users_email" ON users USING btree (email);',
    ]
    utils.runAndCompare(commands1, commands2, expected, done)
  })

  it('should drop an index', function(done) {
    var commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
      'CREATE INDEX users_email ON users (email)',
    ]
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
    ]
    var expected = [
      'DROP INDEX "public"."users_email";',
    ]
    utils.runAndCompare(commands1, commands2, expected, done)
  })

  it('should recreate an index', function(done) {
    var commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
      'ALTER TABLE users ADD COLUMN last_name VARCHAR(255)',
      'CREATE INDEX some_index ON users (first_name)',
    ]
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
      'ALTER TABLE users ADD COLUMN last_name VARCHAR(255)',
      'CREATE INDEX some_index ON users (last_name)',
    ]
    var expected = [
      '-- Index "public"."some_index" needs to be changed',
      'DROP INDEX "public"."some_index";',
      'CREATE INDEX "some_index" ON users USING btree (last_name);',
    ]
    utils.runAndCompare(commands1, commands2, expected, done)
  })

  it('should create a table with an index', function(done) {
    var commands1 = []
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'CREATE INDEX users_email ON users (email)',
    ]
    var expected = [
      'CREATE TABLE "public"."users" (\n  "email" character varying(255) NULL',
      ');',
      'CREATE INDEX "users_email" ON users USING btree (email);',
    ]
    utils.runAndCompare(commands1, commands2, expected, done)
  })

  it('should run as a cli application', function(done) {
    var conString1 = 'postgres://postgres:postgres@localhost/db1'
    var conString2 = 'postgres://postgres:postgres@localhost/db2'

    txain(function(callback) {
      utils.runCommands(['CREATE SEQUENCE seq_name'], [], callback)
    })
    .then(function(arg, callback) {
      exec('node index.js '+conString1+' '+conString2, function(err, stdout, stderr) {
        assert.ifError(err)
        assert.equal(stdout, 'DROP SEQUENCE "public"."seq_name";\n\n')
        done()
      })
    })
    .end(done)
  })

  it('should fail with an erorr', function(done) {
    var conString1 = 'postgres://postgres:postgres@localhost/db1'
    var conString2 = 'postgres://postgres:postgres@localhost/none'

    require('child_process').exec('node index.js '+conString1+' '+conString2, function(err, stdout, stderr) {
      assert.ok(err)
      assert.ok(stderr.indexOf('error: database "none" does not exist') >= 0)
      done()
    })
  })

})
