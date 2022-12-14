# pg-flare

`pg-flare` is a utility library and a command-line application for managing PostgreSQL logical replication.

As of December 19, 2022, I have successfully migrated the prod database from 10 to 14 with this tool. I'll use this tool again when do the migration in the future.

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

**Create a table to write a probe message in a given database to ensure a latest transaction is replicated to the subscriber (ie. `bench` in the example)**:
```sh
./flare create_replication_status_table bench
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

**Execute an external command with a verified publisher and subscriber conninfo**:
```sh
./flare exec env | grep FLARE_CONNINFO
FLARE_CONNINFO_PUBLISHER_HOST=127.0.0.1
FLARE_CONNINFO_PUBLISHER_PORT=5430
FLARE_CONNINFO_SUBSCRIBER_HOST=127.0.0.1
FLARE_CONNINFO_SUBSCRIBER_PORT=5431

# if you want to pass arguments to an external command, use `--` separator
./flare --config ~/tmp/local.yml exec -- sh -c 'env' | grep FLARE_CONNINFO
FLARE_CONNINFO_PUBLISHER_HOST=127.0.0.1
FLARE_CONNINFO_PUBLISHER_PORT=5430
FLARE_CONNINFO_SUBSCRIBER_HOST=127.0.0.1
FLARE_CONNINFO_SUBSCRIBER_PORT=5431
```

## Test Scenario with Amazon RDS

**Launch the entire stack with terraform**:
```
module "rds_test" {
  source  = "github.com/nabeken/study-pg10-logical-replication//tf"

  project_name = "study-rds"

  availability_zones = [
    "ap-northeast-1a", # apne1-az4
    "ap-northeast-1c", # apne1-az1
    "ap-northeast-1d", # apne1-az2
  ]
}
```

**Setup SSH keys in the bastion**:
- Go to the AWS Management Console
- Connect to the bastion via Session Manager
- Run the following commands

  ```sh
  sudo su - ec2-user
  curl https://github.com/<yourname>.keys >> .ssh/authorized_keys
  ```

**Connect to the publisher and the subscriber via the bastion with SSH local port-forwarding over SSM Session Manager**:
```sh
ssh ec2-user@i-<instance-id> \
  -L15432:<publisher>.rds.amazonaws.com:5432 \
  -L35432:<subscriber>.rds.amazonaws.com:5432
```

Make sure you can connec to the RDS instances from your local. The password can be found in the terraform module.
```sh
psql -U postgres -h 127.0.0.1 -p 15432 postgres
postgres=> select version();
                                                 version
----------------------------------------------------------------------------------------------------------
 PostgreSQL 10.21 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 7.3.1 20180712 (Red Hat 7.3.1-12), 64-bit
(1 row)

psql -U postgres -h 127.0.0.1 -p 35432 postgres
postgres=> select version();
                                                 version
---------------------------------------------------------------------------------------------------------
 PostgreSQL 14.4 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 7.3.1 20180712 (Red Hat 7.3.1-12), 64-bit
(1 row)
```

**Create roles**:
- the app user ("app"):

  ```sh
  createuser -U postgres -h 127.0.0.1 -p15432 --login --no-createrole --no-superuser --no-createdb --pwprompt app
  ```

- the database owner user ("dbowner"):

  ```sh
  createuser -U postgres -h 127.0.0.1 -p15432 --login --no-createrole --no-superuser --createdb --pwprompt dbowner
  ```

- the replication user ("repl"):

  ```
  # create a role
  createuser -U postgres -h 127.0.0.1 -p15432 --login --no-createrole --no-superuser --no-createdb --pwprompt repl

  # assing to `rds_replication` role
  cat <<EOF | psql -U postgres -h 127.0.0.1 -p15432 postgres
  GRANT rds_replication TO repl;
  EOF
  ```


**Create a config**:
```
hosts:
  publisher:
    conn:
      superuser: 'postgres'
      superuser_password: '<PASSWORD>'

      db_owner: 'dbowner'
      db_owner_password: 'dbowner'

      repl_user: 'repl'
      repl_user_password: 'repl'

      host: '127.0.0.1'
      host_via_subscriber: '<publisher>.rds.amazonaws.com'

      port: '15432'
      port_via_subscriber: '5432'

      system_identifier: '<identifier>'
  subscriber:
    conn:
      superuser: 'postgres'
      superuser_password: '<PASSWORD>'

      db_owner: 'dbowner'
      db_owner_password: 'dbadmin'

      host: 127.0.0.1
      port: '35432'

      system_identifier: '<identifier>'

publications:
  flare_test:
    pubname: flare-pub

subscriptions:
  flare1:
    dbname: flare_test
    pubname: flare-pub
```

**Verify the connectivity**:
```sh
./flare --config rds_test.yml verify_connectivity
```

**Create a database for testing**:
```sh
./flare create_attack_db --app-user app --base-dsn postgres://dbowner:dbowner@127.0.0.1:15432
./flare --config rds_test.yml create_replication_status_table flare_test
```

**Install some extensions to demonstrate the command**:
```sh
cat <<EOF | psql -U postgres -h 127.0.0.1 -p 15432 flare_test
CREATE EXTENSION pgcrypto;
EOF
```

**Replicate the roles from the publisher to the subscriber**:
```sh
# RDS doesn't allow to dump the password
./flare --config rds_test.yml replicate_roles --no-passwords --strip-options-for-rds
```

**Set the password manually in the subscriber**:
```sh
psql -U postgres -h 127.0.0.1 -p 35432 postgres

\password dbowner

\password app
```

**Grant the superuser CREATE to a given database if the RDS is running on PostgreSQL 10**:
```sh
./flare --config rds_test.yml grant_create --use-db-owner flare_test
```

**Grant the replication user all the privileges on a given database**:
```sh
./flare --config rds_test.yml grant_replication --use-db-owner flare_test
```

**Start writing records against flare_test table... on the bastion**:
```sh
sudo yum install -y docker
sudo systemctl start docker
sudo usermod -aG docker ec2-user
sudo su - ec2-user

docker run --rm -it ghcr.io/nabeken/pg-flare:latest attack --dsn postgres://app:app@<publisher>.rds.amazonaws.com/flare_test
```

**Replicate the schema from the publisher to the subscriber**:
```sh
./flare --config rds_test.yml replicate_schema --use-db-owner flare_test
```

**Confirm all of the extensions are installed in the subscriber**:

```
cat <<EOF | psql -U postgres -h 127.0.0.1 -p35432 flare_test
SELECT * FROM pg_available_extensions WHERE installed_version IS NOT NULL ORDER BY name;
EOF
```

**Monitor the replication**:
```sh
./flare --config rds_test.yml monitor flare_test flare1
```

**Create a publication in the publisher**:
```sh
./flare --config rds_test.yml create_publication flare_test
```

**Create a subscription in the subscriber**:
```sh
./flare --config rds_test.yml create_subscription --use-repl-user flare1
```

**Pause write traffic and wait for the LSN to be advanced in the subscriber**:
```sh
./flare --config rds_test.yml pause_write --app-user app flare_test flare1
```

**Drop the subscription**:
```sh
./flare --config rds_test.yml drop_subscription flare1
```
