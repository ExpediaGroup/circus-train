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
/**
 * The interfaces in this package provide metadata transformations on the table, partition and column statistics of Hive
 * tables.
 * <p>
 * Classes implementing any of these interfaces must be annotated with Spring's {@code @Component} annotation and added
 * to the classpath in order to be loaded by Circus Train. Only 1 instance of an implementation of each interface
 * annotated with {@code @Component} can be on the classpath, otherwise Circus Train will fail to start.
 * </p>
 * <p>
 * If external implementations of these interfaces are found on the classpath then they will be applied to all the
 * replicated tables. There are a few ways of selecting the tables that the transformations should be applied to:
 * <ul>
 * <li>Put the tables in separate configuration files and only add the implementations to the classpath for the
 * appropriate configuration file</li>
 * <li>Filter the tables using the metadata object passed in to the {@code transform()} method of the interface - this
 * is not recommended because it leads to brittle, hard-coded table names etc.</li>
 * </ul>
 * </p>
 * <p>
 * When multiple transformations are required the <a href="https://en.wikipedia.org/wiki/Composite_pattern">composite
 * pattern</a> must be applied and only the composite object must be exposed as a Spring {@code @Component}.
 * </p>
 */
package com.hotels.bdp.circustrain.api.metadata;
