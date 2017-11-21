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
package com.hotels.bdp.circustrain.validation.constraints;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.validation.Constraint;
import javax.validation.OverridesAttribute;
import javax.validation.Payload;
import javax.validation.ReportAsSingleViolation;
import javax.validation.constraints.Pattern;

import com.hotels.bdp.circustrain.validation.constraintvalidators.URIValidator;

/**
 * Validates the annotated string is an URI.
 * <p>
 * The parameters {@code scheme}, {@code host} and {@code port} are matched against the corresponding parts of the
 * {@link URI} and an additional regular expression can be specified using {@code regexp} and {@code flags} to further
 * restrict the matching criteria.
 * </p>
 * <p>
 *
 * @see <a href="http://www.ietf.org/rfc/rfc2396.txt">RFC2396</a>
 * @see org.hibernate.validator.spi.constraintdefinition.ConstraintDefinitionContributor
 */
@Documented
@Constraint(validatedBy = { URIValidator.class })
@Target({ METHOD, FIELD, ANNOTATION_TYPE, CONSTRUCTOR, PARAMETER })
@Retention(RUNTIME)
@ReportAsSingleViolation
@Pattern(regexp = "")
public @interface URI {
  String message() default "{com.hotels.bdp.circustrain.validation.URI.message}";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};

  /**
   * @return the scheme the annotated string must match, e.g. ftp or http. By default any protocol is allowed
   */
  String scheme() default "";

  /**
   * @return the host the annotated string must match, e.g. localhost. By default any host is allowed
   */
  String host() default "";

  /**
   * @return the port the annotated string must match, e.g. 80. By default any port is allowed
   */
  int port() default -1;

  /**
   * @return an additional regular expression the annotated URI must match. The default is any string ('.*')
   */
  @OverridesAttribute(constraint = Pattern.class, name = "regexp")
  String regexp() default ".*";

  /**
   * @return used in combination with {@link #regexp()} in order to specify a regular expression option
   */
  @OverridesAttribute(constraint = Pattern.class, name = "flags")
  Pattern.Flag[] flags() default {};

  /**
   * Defines several {@code @URI} annotations on the same element.
   */
  @Target({ METHOD, FIELD, ANNOTATION_TYPE, CONSTRUCTOR, PARAMETER })
  @Retention(RUNTIME)
  @Documented
  public @interface List {
    URI[] value();
  }
}
