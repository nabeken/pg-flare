# pg-flare

`pg-flare` is a utility library and a command-line application for managing PostgreSQL logical replication.

## Motivation

I found PostgreSQL's logical replication can be used for minimizing downtime for the major upgrade. The whole process must be automated to minimize the downtime. Thus, I need a foundation to build a solution that works for my purpose.

## Design

`flare` doesn't require any runtime configuration to prevent an operation mistake.

## Configuration

`flare` requires a DSN configuration in YAML. It allows to have a pre-defined, pre-validated configuration so `flare` won't touch an unexpected database all the time.

Configuration is designed for a single publisher and a subscriber model but allows to have multiple publication and subscriptions.

```yaml
hosts:
  publisher:
      conn:
        superuser:
        superuser_password:

        db_owner:
        db_owner_password::

        repl_user:
        repl_user_password::

        host:
        host_via_subscriber: # hostname that can be resolved from the subscriber

        port:
        port_via_subscriber: # port that can be accessible from the subscriber

        system_identifier:

  subscriber:
      conn:
        superuser:
        superuser_password:

        db_owner:
        db_owner_password::

        host:
        port:
        system_identifier

publications:
  bench: # dbname
    pubname: bench
    replica_identity_full_tables:
      - pgbench_history

subscriptions:
  bench1: # subname
    dbname: bench
    pubname: bench
```

`system_identifier` is very important. It makes sure of a database you specify matches exactly what you expect. You can get `system_identifier` by using the following query:

```sql
SELECT system_identifier FROM pg_control_system();
```

## Component

- Checking connectivity
- Replicating roles from the publishder to the subscriber
- Replicating schemas from the publisher to the subscriber
- Installing all of the extensions in the publisher
- Creating a publisher
  - "db owner" needs to grant "super user" `CREATE` to create publications
  - "super user" needs to create a publication (ie. only super user can drop a publication)
- Creating a subscriber
- Pausing write traffic against the publisher
- Resuming write traffic in case of emergency
- Checking whether or not WAL has been advanced after all the traffic are paused
- Generating write traffic for testing

## Example

**Verify connectivity to the publisher and subscriber in the config**:
```sh
./flare verify_connectivity
```

**Replicating the roles from the publisher to the subscriber**:
```sh
./flare replicate_roles
```

**Replicating the installed extensions from the publisher to the subscriber in a given database (ie. `bench` in the example)**:
```sh
./flare install_extensions bench
```

**Grant `CREATE` in a given database to a super user (postgres) (ie. `bench` in the example)**:
```sh
./flare grant_create bench
```

**Replicating the schema in a given database (ie. `bench` in the example)**:
```sh
./flare replicate_schema bench
```

**Creating a publication in the publisher for a given database (ie. `bench` in the example)**:
```sh
./flare create_publication bench
```

**Creating a subscription in the subscriber for a given database (ie. `bench` in the example)**:
```sh
./flare create_subscription bench
```

**Generating a test traffic in the `flare_test` database in the publisher**:
```sh
# create a database
./flare create_attack_db --drop-db-before

# run the test (press Ctrl-C to stop)
./flare attack
```

**Pausing write traffic against the database (ie. `bench` in the example)**:
```sh
./flare pause_write bench
```

**Resume write traffic against the database (ie. `bench` in the example)**:
```sh
./flare resume_write bench
```
