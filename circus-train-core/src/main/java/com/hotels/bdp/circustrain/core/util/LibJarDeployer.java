/**
 * Copyright (C) 2016-2019 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.bdp.circustrain.core.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.PrivilegedAction;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

/**
 * Pretty much a copy of the {@code -libjars} implementation in {@link GenericOptionsParser}. I'm sure we can take out
 * some of the marshalling steps, but I'm not entirely sure what is needed in the {@link Configuration} at this time.
 */
public class LibJarDeployer {

  private static final Logger LOG = LoggerFactory.getLogger(LibJarDeployer.class);

  private static class PriviledgedClassLoader implements PrivilegedAction<Void> {
    private final Configuration conf;
    private final URL[] libjars;

    public PriviledgedClassLoader(Configuration conf, URL[] libjars) {
      this.conf = conf;
      this.libjars = libjars;
    }

    @Override
    public Void run() {
      conf.setClassLoader(new URLClassLoader(libjars, conf.getClassLoader()));
      Thread
          .currentThread()
          .setContextClassLoader(new URLClassLoader(libjars, Thread.currentThread().getContextClassLoader()));
      return null; // nothing to return
    }
  }

  public void libjars(Configuration conf, Class<?>... targetClasses) throws IOException {
    String libjarsList = createLibJarList(targetClasses);
    conf.set("tmpjars", validateFiles(libjarsList, conf), "from -libjars command line option");
    // setting libjars in client classpath
    URL[] libjars = GenericOptionsParser.getLibJars(conf);
    if (libjars != null && libjars.length > 0) {
      new PriviledgedClassLoader(conf, libjars).run();
    }
  }

  private String createLibJarList(Class<?>... targetClasses) {
    Set<URI> jarUris = new HashSet<>();
    for (Class<?> targetClass : targetClasses) {
      URI jarUri = JarPathResolver.PROTECTION_DOMAIN.getPath(targetClass);
      LOG.debug("Found libjar for '{}' at '{}'", targetClass, jarUri);
      jarUris.add(jarUri);
    }
    String libjarsList = Joiner.on(',').join(jarUris);
    return libjarsList;
  }

  /**
   * takes input as a comma separated list of files and verifies if they exist. It defaults for file:/// if the files
   * specified do not have a scheme. it returns the paths uri converted defaulting to file:///. So an input of
   * /home/user/file1,/home/user/file2 would return file:///home/user/file1,file:///home/user/file2
   *
   * @param files
   * @return
   */
  private String validateFiles(String files, Configuration conf) throws IOException {
    if (files == null) {
      return null;
    }
    String[] fileArr = files.split(",");
    if (fileArr.length == 0) {
      throw new IllegalArgumentException("File name can't be empty string");
    }
    String[] finalArr = new String[fileArr.length];
    for (int i = 0; i < fileArr.length; i++) {
      String tmp = fileArr[i];
      if (tmp.isEmpty()) {
        throw new IllegalArgumentException("File name can't be empty string");
      }
      String finalPath;
      URI pathURI;
      try {
        pathURI = new URI(tmp);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
      Path path = new Path(pathURI);
      FileSystem localFs = FileSystem.getLocal(conf);
      if (pathURI.getScheme() == null) {
        // default to the local file system
        // check if the file exists or not first
        if (!localFs.exists(path)) {
          throw new FileNotFoundException("File " + tmp + " does not exist.");
        }
        finalPath = path.makeQualified(localFs.getUri(), localFs.getWorkingDirectory()).toString();
      } else {
        // check if the file exists in this file system
        // we need to recreate this filesystem object to copy
        // these files to the file system ResourceManager is running
        // on.
        FileSystem fs = path.getFileSystem(conf);
        if (!fs.exists(path)) {
          throw new FileNotFoundException("File " + tmp + " does not exist.");
        }
        finalPath = path.makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString();
      }
      finalArr[i] = finalPath;
    }
    return StringUtils.arrayToString(finalArr);
  }

}
