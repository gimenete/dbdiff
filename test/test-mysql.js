/* globals describe it */
var dedent = require('dedent')
var conString1 = 'mysql://root:@localhost/db1'
var conSettings2 = {
  dialect: 'mysql',
  username: 'root',
  password: '',
  database: 'db2',
  host: 'localhost'
}
var utils = require('./utils')('mysql', conString1, conSettings2)

describe('MySQL', () => {
  it('should create a table', () => {
    var commands1 = []
    var commands2 = ['CREATE TABLE users (email VARCHAR(255), tags varchar(255))']
    var expected = dedent`
      CREATE TABLE \`users\` (
        \`email\` varchar(255) NULL,
        \`tags\` varchar(255) NULL
      );
    `
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should drop a table', () => {
    var commands1 = ['CREATE TABLE users (email VARCHAR(255))']
    var commands2 = []
    return Promise.resolve()
      .then(() => {
        var expected = 'DROP TABLE `users`;'
        return utils.runAndCompare(commands1, commands2, expected, ['drop'])
      })
      .then(() => {
        var expected = '-- DROP TABLE `users`;'
        return utils.runAndCompare(commands1, commands2, expected, ['safe', 'warn'])
      })
  })

  it('should create a table wih an index', () => {
    var commands1 = []
    var commands2 = ['CREATE TABLE users (id integer primary key auto_increment)']
    var expected = dedent`
      CREATE TABLE \`users\` (
        \`id\` int(11) NOT NULL PRIMARY KEY AUTO_INCREMENT
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
    var expected = 'ALTER TABLE `users` ADD COLUMN `first_name` varchar(255) NULL;'
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
        var expected = 'ALTER TABLE `users` DROP COLUMN `first_name`;'
        return utils.runAndCompare(commands1, commands2, expected, ['drop'])
      })
      .then(() => {
        var expected = '-- ALTER TABLE `users` DROP COLUMN `first_name`;'
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
          -- Previous data type was varchar(200)
          ALTER TABLE \`users\` MODIFY \`first_name\` varchar(255) NULL;
        `
        return utils.runAndCompare(commands1, commands2, expected, ['drop', 'warn'])
      })
      .then(() => {
        var expected = dedent`
          -- Previous data type was varchar(200)
          -- ALTER TABLE \`users\` MODIFY \`first_name\` varchar(255) NULL;
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
        var expected = 'ALTER TABLE `users` MODIFY `first_name` varchar(255) NOT NULL;'
        return utils.runAndCompare(commands1, commands2, expected, ['drop', 'warn'])
      })
      .then(() => {
        var expected = '-- ALTER TABLE `users` MODIFY `first_name` varchar(255) NOT NULL;'
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
    var expected = 'ALTER TABLE `users` MODIFY `first_name` varchar(255) NULL;'
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should create an index', () => {
    var commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)'
    ]
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
      'CREATE INDEX users_email ON `users` (email)'
    ]
    var expected = 'CREATE INDEX `users_email` USING BTREE ON `users` (`email`);'
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
    var expected = 'DROP INDEX `users_email` ON `users`;'
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should recreate an index', () => {
    var commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
      'ALTER TABLE users ADD COLUMN last_name VARCHAR(255)',
      'CREATE INDEX some_index ON `users` (first_name)'
    ]
    var commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
      'ALTER TABLE users ADD COLUMN last_name VARCHAR(255)',
      'CREATE INDEX some_index ON `users` (last_name)'
    ]
    var expected = dedent`
      -- Index \`some_index\` needs to be changed

      DROP INDEX \`some_index\` ON \`users\`;

      CREATE INDEX \`some_index\` USING BTREE ON \`users\` (\`last_name\`);
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
      CREATE TABLE \`users\` (
        \`email\` varchar(255) NULL
      );

      CREATE INDEX \`users_email\` USING BTREE ON \`users\` (\`email\`);
    `
    return utils.runAndCompare(commands1, commands2, expected)
  })

  it('should support all constraint types', () => {
    var commands1 = []
    var commands2 = [
      'CREATE TABLE users (id bigint primary key auto_increment, email VARCHAR(255));',
      'CREATE TABLE items (id bigint primary key auto_increment, name VARCHAR(255), user_id bigint);',
      'ALTER TABLE users ADD CONSTRAINT email_unique UNIQUE (email);',
      'ALTER TABLE items ADD CONSTRAINT items_fk FOREIGN KEY (user_id) REFERENCES users (id);'
    ]
    var expected = dedent`
      CREATE TABLE \`items\` (
        \`id\` bigint(20) NOT NULL PRIMARY KEY AUTO_INCREMENT,
        \`name\` varchar(255) NULL,
        \`user_id\` bigint(20) NULL
      );

      CREATE TABLE \`users\` (
        \`id\` bigint(20) NOT NULL PRIMARY KEY AUTO_INCREMENT,
        \`email\` varchar(255) NULL
      );

      ALTER TABLE \`items\` ADD CONSTRAINT \`items_fk\` FOREIGN KEY (\`user_id\`) REFERENCES \`users\` (\`id\`);

      ALTER TABLE \`users\` ADD CONSTRAINT \`email_unique\` UNIQUE (\`email\`);
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
          ALTER TABLE \`users\` DROP INDEX \`a_unique_constraint\`;

          ALTER TABLE \`users\` ADD CONSTRAINT \`a_unique_constraint\` UNIQUE (\`api_key\`);
        `
        return utils.runAndCompare(commands1, commands2, expected, ['warn', 'drop'])
      })
      .then(() => {
        var expected = dedent`
          ALTER TABLE \`users\` DROP INDEX \`a_unique_constraint\`;

          -- ALTER TABLE \`users\` ADD CONSTRAINT \`a_unique_constraint\` UNIQUE (\`api_key\`);
        `
        return utils.runAndCompare(commands1, commands2, expected, ['safe'])
      })
  })
})
