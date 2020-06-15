![Circus Train.](circus-train.png "Moving Hive data between sites.")

# Table Schema Evolution

## Overview
Circus Train replicates Hive tables, both data and metadata, on request. In doing so, Circus Train's support for schema evolution roughly follows what is supported by Hive.

Broadly, if your schema change is backwards compatible, Circus Train is likely to support it. However, as Circus Train's feature set is continually growing, supported schema evolutions are therefore subject to change. The following table describes exactly what schema evolutions are supported.

## Supported Evolutions (Parquet table format)

Schema Evolution|Supported by Circus Train|Note
|----|----|----|
Additional field|Yes||
Removed field|Yes||
Renamed field|Yes|Replication will succeed, but previous data will remain in the original column name|
Add default value to field|Yes||
Remove default value from field|Yes||
Make field nullable|Yes||
Make field union|Unknown|<em>union type not supported by Parquet</em>|
Add type to union|Unknown|<em>union type not supported by Parquet</em>|
Remove type from union|Unknown|<em>union type not supported by Parquet</em>|
Promote int to long, float or double |Yes||
Promote long to float or double |Yes||
Promote float to double |Yes||
Demote long, float or double to int |No||
Demote double or float to long
Demote double to float |No||
