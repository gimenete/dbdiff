/* globals describe it */
var dedent = require('dedent')

var conString1 = 'postgres://postgres:postgres@localhost/db1'
var conSettings2 = {
  dialect: 'postgres',
  username: 'postgres',
  password: 'postgres',
  database: 'db2',
  host: 'localhost',
  dialectOptions: {
    ssl: false
  }
}
var utils = require('./utils')('postgres', conString1, conSettings2)

describe('Postgresql', () => {
  it('should create a table', () => {
    var commands1 = []
    var commands2 = ['CREATE TABLE users (email VARCHAR(255), tags varchar(255)[])']
    var expected = dedent`
      CREATE TABLE "public"."users" (
        "email" character varying(255) NULL,
        "tags" varchar[] NULL
      );
    `
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should drop a table', () => {
    var commands1 = ['CREATE TABLE users (email VARCHAR(255))']
    var commands2 = []
    return Promise.resolve()
      .then(() => {
        var expected = 'DROP TABLE "public"."users";'
        return utils.runAndCompare(commands1, commands2, expected, ['drop'])
      })
      .then(() => {
        var expected = '-- DROP TABLE "public"."users";'
        return utils.runAndCompare(commands1, commands2, expected, ['safe', 'warn'])
      })
  })

  it('should create a table wih a serial sequence', () => {
    var commands1 = []
    var commands2 = ['CREATE TABLE users (id serial)']
    var expected = dedent`
      CREATE SEQUENCE "public"."users_id_seq" INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 NO CYCLE;

      CREATE TABLE "public"."users" (
        "id" integer DEFAULT nextval('users_id_seq'::regclass) NOT NULL
      );
    `
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should add a column to a table', () => {
    var commands1 = ['CREATE TABLE users (email VARCHAR(255))']
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)'
    ]
    var expected = 'ALTER TABLE "public"."users" ADD COLUMN "first_name" character varying(255) NULL;'
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
    return Promise.resolve()
      .then(() => {
        var expected = 'ALTER TABLE "public"."users" DROP COLUMN "first_name";'
        return utils.runAndCompare(commands1, commands2, expected, ['drop'])
      })
      .then(() => {
        var expected = '-- ALTER TABLE "public"."users" DROP COLUMN "first_name";'
        return utils.runAndCompare(commands1, commands2, expected, ['safe', 'warn'])
      })
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
    return Promise.resolve()
      .then(() => {
        var expected = dedent`
          -- Previous data type was character varying(200)
          ALTER TABLE "public"."users" ALTER COLUMN "first_name" SET DATA TYPE character varying(255);
        `
        return utils.runAndCompare(commands1, commands2, expected, ['drop', 'warn'])
      })
      .then(() => {
        var expected = dedent`
          -- Previous data type was character varying(200)
          -- ALTER TABLE "public"."users" ALTER COLUMN "first_name" SET DATA TYPE character varying(255);
        `
        return utils.runAndCompare(commands1, commands2, expected, ['safe'])
      })
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
    return Promise.resolve()
      .then(() => {
        var expected = 'ALTER TABLE "public"."users" ALTER COLUMN "first_name" SET NOT NULL;'
        return utils.runAndCompare(commands1, commands2, expected, ['drop', 'warn'])
      })
      .then(() => {
        var expected = '-- ALTER TABLE "public"."users" ALTER COLUMN "first_name" SET NOT NULL;'
        return utils.runAndCompare(commands1, commands2, expected, ['safe'])
      })
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
    var expected = 'ALTER TABLE "public"."users" ALTER COLUMN "first_name" DROP NOT NULL;'
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should create a sequence', () => {
    var commands1 = []
    var commands2 = ['CREATE SEQUENCE seq_name']
    var expected = 'CREATE SEQUENCE "public"."seq_name" INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 NO CYCLE;'
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should drop a sequence', () => {
    var commands1 = ['CREATE SEQUENCE seq_name']
    var commands2 = []
    var expected = 'DROP SEQUENCE "public"."seq_name";'
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
    var expected = 'CREATE INDEX "users_email" ON "public"."users" USING btree ("email");'
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
    var expected = 'DROP INDEX "public"."users_email";'
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
    var expected = dedent`
      -- Index "public"."some_index" needs to be changed

      DROP INDEX "public"."some_index";

      CREATE INDEX "some_index" ON "public"."users" USING btree ("last_name");
    `
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should create a table with an index', () => {
    var commands1 = []
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'CREATE INDEX users_email ON users (email)'
    ]
    var expected = dedent`
      CREATE TABLE "public"."users" (
        "email" character varying(255) NULL
      );

      CREATE INDEX "users_email" ON "public"."users" USING btree ("email");
    `
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should support all constraint types', () => {
    var commands1 = []
    var commands2 = [
      'CREATE TABLE users (id serial, email VARCHAR(255));',
      'CREATE TABLE items (id serial, name VARCHAR(255), user_id bigint);',
      'ALTER TABLE users ADD CONSTRAINT users_pk PRIMARY KEY (id);',
      'ALTER TABLE users ADD CONSTRAINT email_unique UNIQUE (email);',
      'ALTER TABLE items ADD CONSTRAINT items_fk FOREIGN KEY (user_id) REFERENCES users (id);'
    ]
    var expected = dedent`
      CREATE SEQUENCE "public"."users_id_seq" INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 NO CYCLE;

      CREATE SEQUENCE "public"."items_id_seq" INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 NO CYCLE;

      CREATE TABLE "public"."users" (
        "id" integer DEFAULT nextval('users_id_seq'::regclass) NOT NULL,
        "email" character varying(255) NULL
      );

      CREATE TABLE "public"."items" (
        "id" integer DEFAULT nextval('items_id_seq'::regclass) NOT NULL,
        "name" character varying(255) NULL,
        "user_id" bigint NULL
      );

      ALTER TABLE "public"."users" ADD CONSTRAINT "email_unique" UNIQUE ("email");

      ALTER TABLE "public"."users" ADD CONSTRAINT "users_pk" PRIMARY KEY ("id");

      ALTER TABLE "public"."items" ADD CONSTRAINT "items_fk" FOREIGN KEY ("user_id") REFERENCES "users" ("id");
    `
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should support existing constriants with the same name', () => {
    var commands1 = [
      'CREATE TABLE users (email VARCHAR(255), api_key VARCHAR(255));',
      'ALTER TABLE users ADD CONSTRAINT a_unique_constraint UNIQUE (email);'
    ]
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255), api_key VARCHAR(255));',
      'ALTER TABLE users ADD CONSTRAINT a_unique_constraint UNIQUE (api_key);'
    ]
    return Promise.resolve()
      .then(() => {
        var expected = dedent`
          ALTER TABLE "public"."users" DROP CONSTRAINT "a_unique_constraint";

          ALTER TABLE "public"."users" ADD CONSTRAINT "a_unique_constraint" UNIQUE ("api_key");
        `
        return utils.runAndCompare(commands1, commands2, expected, ['warn', 'drop'])
      })
      .then(() => {
        var expected = dedent`
          ALTER TABLE "public"."users" DROP CONSTRAINT "a_unique_constraint";

          -- ALTER TABLE "public"."users" ADD CONSTRAINT "a_unique_constraint" UNIQUE ("api_key");
        `
        return utils.runAndCompare(commands1, commands2, expected, ['safe'])
      })
  })
})
