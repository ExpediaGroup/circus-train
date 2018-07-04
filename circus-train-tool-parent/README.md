# Circus Train Tool

Circus Train Tool Parent is a sub-module for debug and maintenance tools.
* **circus-train-comparison-tool:** Tool that performs data and metadata comparisons, useful for debugging the "Hive Diff" feature.
* **circus-train-filter-tool:** Tool that checks partition filter expressions in table replication configurations.
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
	

### Note
In order to run the circus-train-tools the user must install circus train, and set the bash variable CIRCUS_TRAIN_HOME to the base location of the circus-train folders.
