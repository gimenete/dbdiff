/* globals describe beforeEach it */
var utils = require('./utils')
var assert = require('assert')
var childProcess = require('child_process')

const exec = (cmd) => {
  return new Promise((resolve, reject) => {
    childProcess.exec(cmd, (err, stdout, stderr) => {
      err && !stderr ? reject(err) : resolve({ stdout, stderr })
    })
  })
}

describe('dbdiff.compareDatabases', () => {
  beforeEach(() => utils.resetDatabases())

  it('should create a table', () => {
    var commands1 = []
    var commands2 = ['CREATE TABLE users (email VARCHAR(255), tags varchar(255)[])']
    var expected = [
      'CREATE TABLE "public"."users" (\n  "email" character varying(255) NULL,\n  "tags" varchar[] NULL\n);'
    ]
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should drop a table', () => {
    var commands1 = ['CREATE TABLE users (email VARCHAR(255))']
    var commands2 = []
    var expected = [
      'DROP TABLE "public"."users";'
    ]
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should create a table wih an index', () => {
    var commands1 = []
    var commands2 = ['CREATE TABLE users (id serial)']
    var expected = [
      'CREATE SEQUENCE "public"."users_id_seq" INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 NO CYCLE;',
      'CREATE TABLE "public"."users" (\n  "id" integer DEFAULT nextval(\'users_id_seq\'::regclass) NOT NULL\n);'
    ]
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should add a column to a table', () => {
    var commands1 = ['CREATE TABLE users (email VARCHAR(255))']
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)'
    ]
    var expected = [
      'ALTER TABLE "public"."users" ADD COLUMN "first_name" character varying(255) NULL;'
    ]
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should drop a column from a table', () => {
    var commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)'
    ]
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))'
    ]
    var expected = [
      'ALTER TABLE "public"."users" DROP COLUMN "first_name";'
    ]
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should change the type of a column', () => {
    var commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(200)'
    ]
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)'
    ]
    var expected = [
      '-- Previous data type was character varying(200)',
      'ALTER TABLE "public"."users" ALTER COLUMN "first_name" SET DATA TYPE character varying(255);'
    ]
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should change a column to not nullable', () => {
    var commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)'
    ]
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255) NOT NULL'
    ]
    var expected = [
      'ALTER TABLE "public"."users" ALTER COLUMN "first_name" SET NOT NULL;'
    ]
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should change a column to nullable', () => {
    var commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255) NOT NULL'
    ]
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)'
    ]
    var expected = [
      'ALTER TABLE "public"."users" ALTER COLUMN "first_name" DROP NOT NULL;'
    ]
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should create a sequence', () => {
    var commands1 = []
    var commands2 = ['CREATE SEQUENCE seq_name']
    var expected = [
      'CREATE SEQUENCE "public"."seq_name" INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 NO CYCLE;'
    ]
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should drop a sequence', () => {
    var commands1 = ['CREATE SEQUENCE seq_name']
    var commands2 = []
    var expected = [
      'DROP SEQUENCE "public"."seq_name";'
    ]
    return utils.runAndCompare(commands1, commands2, expected)
  })

  // TODO: update a sequence

  it('should create an index', () => {
    var commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)'
    ]
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
      'CREATE INDEX users_email ON "users" (email)'
    ]
    var expected = [
      'CREATE INDEX "users_email" ON "public"."users" USING btree (email);'
    ]
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should drop an index', () => {
    var commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
      'CREATE INDEX users_email ON users (email)'
    ]
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)'
    ]
    var expected = [
      'DROP INDEX "public"."users_email";'
    ]
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should recreate an index', () => {
    var commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
      'ALTER TABLE users ADD COLUMN last_name VARCHAR(255)',
      'CREATE INDEX some_index ON "users" (first_name)'
    ]
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
      'ALTER TABLE users ADD COLUMN last_name VARCHAR(255)',
      'CREATE INDEX some_index ON "users" (last_name)'
    ]
    var expected = [
      '-- Index "public"."some_index" needs to be changed',
      'DROP INDEX "public"."some_index";',
      'CREATE INDEX "some_index" ON "public"."users" USING btree (last_name);'
    ]
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should create a table with an index', () => {
    var commands1 = []
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'CREATE INDEX users_email ON users (email)'
    ]
    var expected = [
      'CREATE TABLE "public"."users" (\n  "email" character varying(255) NULL\n);',
      'CREATE INDEX "users_email" ON "public"."users" USING btree (email);'
    ]
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should run as a cli application', () => {
    var conString1 = 'postgres://postgres:postgres@localhost/db1'
    var conString2 = 'postgres://postgres:postgres@localhost/db2'

    return utils.runCommands(['CREATE SEQUENCE seq_name'], [])
      .then(() => exec(`node index.js ${conString1} ${conString2}`))
      .then((result) => {
        var { stdout } = result
        assert.equal(stdout, 'DROP SEQUENCE "public"."seq_name";\n\n')
      })
  })

  it('should fail with an erorr', () => {
    var conString1 = 'postgres://postgres:postgres@localhost/db1'
    var conString2 = 'postgres://postgres:postgres@localhost/none'

    return exec(`node index.js ${conString1} ${conString2}`)
      .then((result) => {
        var { stderr } = result
        assert.ok(stderr.indexOf('error: database "none" does not exist') >= 0)
      })
  })
})
