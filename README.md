# Snowflake

A collection of Snowflake connectors and plugins.

Following plugins are available in this repository.

  * Snowflake Batch Source
  * Snowflake Sink
  * Snowflake Run SQL Action
  * Snowflake Cloud Storage To Snowflake Action
  * Snowflake Snowflake To Cloud Storage Action

# Integration tests

By default all tests will be skipped, since Snowflake credentials are needed.

Instructions to enable the tests:
 1. Create/use existing Snowflake account.
 2. Create database for testing.
 3. Run the tests using the command below:

```
mvn clean test -Dsnowflake.test.account.name= -Dsnowflake.test.database= -Dsnowflake.test.schema= -Dsnowflake.test.username= -Dsnowflake.test.password=
```
**snowflake.test.account.name:** Full name of Snowflake account.

**snowflake.test.database:** Database name to connect to.

**snowflake.test.schema:** Schema name to connect to.

**snowflake.test.username:** User identity for connecting to the specified database.

**snowflake.test.password:** Password to use to connect to the specified database.
