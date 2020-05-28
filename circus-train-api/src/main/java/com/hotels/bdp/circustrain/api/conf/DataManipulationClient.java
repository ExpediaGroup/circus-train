package com.hotels.bdp.circustrain.api.conf;

/**
 * A client which when implemented will delete a specified path.
 */
public interface DataManipulationClient {

  public void delete(String path);

}
