---
title: cutover reference
headcontent: yb-voyager cutover
linkTitle: cutover
description: YugabyteDB Voyager cutover reference
menu:
  preview_yugabyte-voyager:
    identifier: voyager-cutover-initiate
    parent: cutover-archive
    weight: 110
type: docs
---

This page describes the following cutover commands:

- [cutover initiate](#cutover-initiate)
- [cutover status](#cutover-status)

### cutover initiate

Initiate [cutover](../../../migrate/live-migrate/#cut-over-to-the-target) to the YugabyteDB database.

#### Syntax

```text
Usage: yb-voyager cutover initiate [ <arguments> ... ]
```

#### Arguments

The valid *arguments* for cutover initiate are described in the following table:

| Argument | Description/valid options |
| :------- | :------------------------ |
| -e, --export-dir <path> | Path to the export directory. This directory is a workspace used to store exported schema DDL files, export data files, migration state, and a log file.|
| -h, --help | Command line help for cutover initiate. |

#### Example

```sh
yb-voyager cutover initiate --export-dir /dir/export-dir
```

### cutover status

Shows the status of the cutover to the YugabyteDB database. Status can be INITIATED, NOT INITIATED, or COMPLETED.

## Syntax

```text
Usage: yb-voyager cutover status [ <arguments> ... ]
```

### Arguments

The valid *arguments* for cutover status are described in the following table:

| Argument | Description/valid options |
| :------- | :------------------------ |
| -e, --export-dir <path> | Path to the export directory. This directory is a workspace used to store exported schema DDL files, export data files, migration state, and a log file.|
| -h, --help | Command line help for cutover status. |

#### Example

```sh
yb-voyager cutover status --export-dir /dir/export-dir
