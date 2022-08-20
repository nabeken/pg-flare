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
