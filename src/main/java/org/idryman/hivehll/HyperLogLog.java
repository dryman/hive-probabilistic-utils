package org.idryman.hivehll;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/*
 * Copyright 2015 Felix Chern
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

@Description(
    name = "hll",
    value = "_FUNC_(cols+) - Calculate the cardinality of the given columns.\n"+
    "The output is in binary form, need to use hll_human to convert it to human readable format",
    extended = "Example:\n" +
    "    -- estimate the cardinality of SELECT * FROM src GROUP BY col1, col2;\n" +
    "    SELECT hll_human(hll(col1, col2)) from src;\n\n" +
    "    -- create hyperloglog cache per hour\n"+
    "    FROM input_table src\n" +
    "    INSERT OVERWRITE TABLE hll_cache PARTITION (d='2015-03-01',h='00')\n"+
    "    SELECT hll(col1,col2) FROM src WHERE d='2015-03-01' AND h='00'\n" +
    "    INSERT OVERWRITE TABLE hll_cache PARTITION (d='2015-03-01',h='01')\n"+
    "    SELECT hll(col1,col2) FROM src WHERE d='2015-03-01' AND h='01'\n\n" +
    "    -- read the cache and calculate the cardinality of full day\n" +
    "    SELECT hll_human(hll(hll_col)) from hll_cache WHERE d='2015-03-01;'\n\n" +
    "    -- unpack hive hll struct and make it readable by postgres-hll, js-hll developed by Aggregate Knowledge, Inc.\n" +
    "    SELECT hll_unpack(hll_col) from hll_cache WHERE d='2015-03-01';"
    )
public class HyperLogLog implements GenericUDAFResolver2{

  public GenericUDAFEvaluator getEvaluator(TypeInfo[] arg0)
      throws SemanticException {
    // TODO Auto-generated method stub
    return null;
  }

  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo arg0)
      throws SemanticException {
    // TODO Auto-generated method stub
    return null;
  }

}
