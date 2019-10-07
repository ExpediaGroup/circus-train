/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.hotels.bdp.circustrain.core.conf;

import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;

import java.lang.reflect.Method;

import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

/**
 * Parser that applies a standard set of functions defined in {@link ExpressionParserFunctions} to expression strings.
 */
@Component
public class SpringExpressionParser {

  private final ExpressionParser parser = new SpelExpressionParser();
  private final StandardEvaluationContext evalContext = new StandardEvaluationContext();
  private final TemplateParserContext templateContext = new TemplateParserContext();

  SpringExpressionParser() {
    this(ExpressionParserFunctions.class);
  }

  SpringExpressionParser(Class<?>... functionHolders) {
    for (Class<?> functionHolder : functionHolders) {
      for (Method method : ReflectionUtils.getAllDeclaredMethods(functionHolder)) {
        if (isPublic(method.getModifiers()) && isStatic(method.getModifiers())) {
          evalContext.registerFunction(method.getName(), method);
        }
      }
    }
  }

  /**
   * Applies any defined functions to the passed expression string and returns the result of this.
   *
   * @param expressionString
   * @return
   */
  public String parse(String expressionString) {
    if (expressionString != null) {
      Expression expression = parser.parseExpression(expressionString, templateContext);
      expressionString = expression.getValue(evalContext).toString();
    }
    return expressionString;
  }

}
