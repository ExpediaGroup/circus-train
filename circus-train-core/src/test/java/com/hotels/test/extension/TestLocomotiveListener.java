/**
 * Copyright (C) 2016-2017 Expedia Inc.
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
package com.hotels.test.extension;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.hotels.bdp.circustrain.api.CompletionCode;
import com.hotels.bdp.circustrain.api.event.EventReplicaCatalog;
import com.hotels.bdp.circustrain.api.event.EventSourceCatalog;
import com.hotels.bdp.circustrain.api.event.LocomotiveListener;
import com.hotels.test.extension.TestConfiguration.TestBean;

// Required for CircusTrainTest.singleYmlFileWithUserExtension
@Component
public class TestLocomotiveListener implements LocomotiveListener {
  public static TestBean testBean;

  @Autowired
  public TestLocomotiveListener(TestBean testBean) {
    TestLocomotiveListener.testBean = testBean;
  }

  @Override
  public void circusTrainStartUp(String[] args, EventSourceCatalog sourceCatalog, EventReplicaCatalog replicaCatalog) {}

  @Override
  public void circusTrainShutDown(CompletionCode completionCode, Map<String, Long> metrics) {}
}
