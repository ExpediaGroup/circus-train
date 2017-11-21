# Circus Train Tool

Circus Train Tool Parent is a sub-module for debug and maintenance tools.
* **circus-train-comparison-tool:** Tool that performs data and metadata comparisons, useful for debugging the "Hive Diff" feature.
* **circus-train-filter-tool:** Tool that checks partition filter expressions in table replication configurations.
* **circus-train-vacuum-tool:** Tool that removes data orphaned by a bug in Circus Train versions prior to 2.0.0
* **circus-train-tool:** Packages tools as TGZ.
* **circus-train-tool-core:** Code common across tool implementations.


## Circus Train Comparison Tool

A tool to compare differences between source and replica tables


### Usage

Run with your respective replication YAML configuration file:

    $CIRCUS_TRAIN_TOOL_HOME/bin/compare-tables.sh \
      --config=<your-config>.yml \
      --outputFile=<output_file>
      
      
## Circus Train filter testing tool

### Usage
        $CIRCUS_TRAIN_TOOL_HOME/bin/check-filters.sh --config=/path/to/config/file.yml
        
### Example

	Source catalog:        production
	Source MetaStore URIs: thrift://host1:9083,thrift://host2:9083
	Source table:          my_db.my_table
	Partition expression:  local_date >= '#{#nowEuropeLondon().minusDays(3).toString("yyyy-MM-dd")}'
	Partition filter:      local_date >= '2016-06-07'
	Partition limit:       1000
	Partition(s) fetched:  [[2016-06-07, 0], [2016-06-07, 10]
	
	
## Circus Train Vacuum Tool

A tool to remove data incorrectly orphaned by Circus Train in versions prior to 2.0.0. The necessity of this project will hopefully decrease rapidly.

### Warning
Please upgrade your Circus Train installation to version 2.0.0 or greater before attempting to use this tool.

### Usage

Run with your respective replication YAML configuration file:

    $CIRCUS_TRAIN_TOOL_HOME/bin/vacuum.sh \
      --config=<your-config>.yml \
      [--dry-run=true] \
      [--partition-batch-size=1000] \
      [--expected-path-count=10000]

Vacuum looks for any files and folders in the data locations of your replicated tables that are not referenced in either the metastore or Circus Train's housekeeping database. Any paths discovered are again scheduled for removal via the housekeeping process. The respective files and folders will then be removed at a time determined by the specific configuration of your housekeeping process.

We use the housekeeping process for data removal in this scenario as it has useful logic for determining when ancestral paths can also be removed.

The `dry-run` option allows you to observe the status of paths on the file system, the metastore, and the housekeeping database without performing any destructive changes. The partition-batch-size and expected-path-count allow you to tune memory demands should you hit heap limits with large numbers of partitions.

### Note
In order to run the circus-train-tools the user must install circus train, and set the bash variable CIRCUS_TRAIN_HOME to the base location of the circus-train folders.