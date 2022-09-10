# pg-flare

`pg-flare` is a utility library and a command-line application for managing PostgreSQL logical replication.

## Motivation

I found PostgreSQL's logical replication can be used for minimizing downtime for the major upgrade. The whole process must be automated to minimize the downtime. Thus, I need a foundation to build a solution that works for my purpose.

## Component

- Connection management for publisher and subscriber
- Checking connectivity
- Generating write traffic for testing
  - should generate `INSERT`, `UPDATE` and `DELETE`
- Replicating roles
- Replicating schemas
- Creating a publisher and subscriber
- Monitoring the replication
- Pausing write traffic
- Checking whether or not write traffic is paused
- Resuming write traffic

## Configuration

`flare` requires a DSN configuration in YAML. It allows to have a pre-defined, pre-validated configuration so `flare` won't touch an unexpected database all the time.

```yaml
hosts:
  publisher:
      conn:
        user:
        host:
        port:
        password:
        system_identifier:
  subscriber:
      conn:
        user:
        host:
        port:
        password:
        system_identifier
```

`system_identifier` is very important. It makes sure of a database you specify matches exactly matches what you expect. You can get `system_identifier` by using the following query:

```sql
SELECT system_identifier FROM pg_control_system();
```

## Example

**Replicating the roles**:
```sh
./flare replicate_roles \
  --src-super-user-dsn postgres://postgres:postgres@localhost:5430 \
  --dst-super-user-dsn postgres://postgres:postgres@localhost:5431
```

**Replicating the schema in `bench` database**:
```sh
./flare replicate_schema \
  --src-super-user-dsn postgres://postgres:postgres@localhost:5430 \
  --dst-super-user-dsn postgres://postgres:postgres@localhost:5431 \
  bench
```
