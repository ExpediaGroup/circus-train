package com.hotels.bdp.circustrain.core.annotation;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Table;

import com.hotels.bdp.circustrain.api.CircusTrainException;

public interface TableAnnotator {

  TableAnnotator NULL_TABLE_ANNOTATOR = new TableAnnotator() {
    @Override
    public void annotateTable(Table table, Map<String, String> orphanedDataOptions)
      throws CircusTrainException {
      //do nothing
    }
  };

  void annotateTable(Table table, Map<String, String> orphanedDataOptions);

}
