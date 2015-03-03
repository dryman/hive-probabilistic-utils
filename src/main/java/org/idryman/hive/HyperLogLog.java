package org.idryman.hive;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import net.agkn.hll.HLL;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.serde2.lazy.LazyBinary;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

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


/*
 * Inspired by MLnick/hive-udf
 */
@Description(
    name = "hll",
    value = "_FUNC_(cols+) - Calculate the cardinality of the given columns.\n"+
    "    returns a struct of {cardinality: long, signature: binary}\n"+
    "    The signature is a binary value that can be parsed by java-hll,\n    postres-hll, and js-hll developed by Aggregate Knowledge, Inc.",
    extended = "Example:\n" +
    "    -- estimate the cardinality of SELECT * FROM src GROUP BY col1, col2;\n" +
    "    SELECT hll(col1, col2).cardinality from src;\n\n" +
    "    -- create hyperloglog cache per hour\n"+
    "    FROM input_table src\n" +
    "    INSERT OVERWRITE TABLE hll_cache PARTITION (d='2015-03-01',h='00')\n"+
    "    SELECT hll(col1,col2) FROM src WHERE d='2015-03-01' AND h='00'\n" +
    "    INSERT OVERWRITE TABLE hll_cache PARTITION (d='2015-03-01',h='01')\n"+
    "    SELECT hll(col1,col2) FROM src WHERE d='2015-03-01' AND h='01'\n\n" +
    "    -- read the cache and calculate the cardinality of full day\n" +
    "    SELECT hll(hll_col).cardinality from hll_cache WHERE d='2015-03-01;'\n\n" +
    "    -- unpack hive hll struct and make it readable by postgres-hll, js-hll developed by Aggregate Knowledge, Inc.\n" +
    "    SELECT hll_col.signature from hll_cache WHERE d='2015-03-01';"
    )
public class HyperLogLog implements GenericUDAFResolver2{
  private final static String CARDINALITY = "cardinality";
  private final static String SIGNATURE = "signature";

  public GenericUDAFEvaluator getEvaluator(TypeInfo[] arg0)
      throws SemanticException {
    // TODO Auto-generated method stub
    return null;
  }

  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
      throws SemanticException {
    ObjectInspector[] inspectors = info.getParameterObjectInspectors();
    
    if (inspectors.length == 0) {
      throw new UDFArgumentException("Must have argument");
    }
    
    /**
     * case 1: can accept one hyperloglog struct
     * TODO refactor error message so it's more informative to user
     */
    if(inspectors[0] instanceof StructObjectInspector) {
      List<? extends StructField> fields = ((StructObjectInspector) inspectors[0]).getAllStructFieldRefs();
      assert fields.size() == 2;
      if (CARDINALITY.equals(fields.get(0).getFieldName())) {
        assert SIGNATURE.equals(fields.get(1).getFieldName());
        assert inspectors.length == 1;
        return new HLLEvaluator();
      } else {
        assert SIGNATURE.equals(fields.get(0).getFieldName());
        assert CARDINALITY.equals(fields.get(1).getFieldName());
        assert inspectors.length == 1;
        /*
         * assert would fail if you try to feed it a random struct
         */
        return new HLLEvaluator(); 
      }
    }
    
    /**
     * TODO refactor error message so it's more informative to user
     */
    for (ObjectInspector oi : inspectors) {
      assert oi instanceof PrimitiveObjectInspector;
    }
    
    return new HLLEvaluator();
  }

  @UDFType(distinctLike=true)
  public static class HLLEvaluator extends GenericUDAFEvaluator {
    HLLMode hll_mode;
    StructObjectInspector hllOI;
    BinaryObjectInspector intermediateOI;
    List<PrimitiveObjectInspector> primitiveOIs;
    ByteArrayOutputStream baos;
    DataOutputStream dos;
    int SEED = 123456;
    HashFunction hashFunc;
    
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      ObjectInspector ret;
      
      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        if (parameters[0] instanceof StructObjectInspector) {
          hllOI = (StructObjectInspector) parameters[0];
          hll_mode = HLLMode.MERGING;
        } else {
          primitiveOIs = Lists.newArrayList();
          for (ObjectInspector param : parameters) {
            primitiveOIs.add((PrimitiveObjectInspector) param);
          }
          baos = new ByteArrayOutputStream();
          dos = new DataOutputStream(baos);
          hashFunc = Hashing.murmur3_128(SEED);
          hll_mode = HLLMode.HASHING;
        }
      } else {
        intermediateOI = (BinaryObjectInspector) parameters[0];
      }
      
      if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
        ret = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
      } else {
        List<String> field_names = Lists.newArrayList(CARDINALITY,SIGNATURE);
        List<ObjectInspector> field_ois = Lists.newArrayList();
        field_ois.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        field_ois.add(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
        ret = ObjectInspectorFactory.getStandardStructObjectInspector(field_names, field_ois);
      }
      
      return ret;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new HLLBuffer();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      // no need to reset buffer
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      HLLBuffer hllbuf = (HLLBuffer) agg;
      switch(hll_mode){
      case HASHING:
        try {
          // TODO Need to test the performance between this and typed primitives
          for (int i=0; i<parameters.length; i++) {
            Writable writable = (Writable)primitiveOIs.get(i).getPrimitiveWritableObject(parameters[i]);
            if (writable!=null)
              writable.write(dos);
          }
          dos.flush();
          hllbuf.hll.addRaw(hashFunc.hashBytes(baos.toByteArray()).asLong());
          baos.reset();
        } catch (IOException e) {
          throw new HiveException(e);
        }
        break;
      case MERGING:
        LazyBinary lb = (LazyBinary) hllOI.getStructFieldData(
            parameters[0], 
            hllOI.getStructFieldRef(SIGNATURE));
        HLL other = HLL.fromBytes(lb.getWritableObject().copyBytes());
        hllbuf.hll.union(other);
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      HLLBuffer hllbuf = (HLLBuffer)agg;
      return new BytesWritable(hllbuf.hll.toBytes());
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
      HLLBuffer hllbuf = (HLLBuffer)agg;
      if (partial!=null){
        BytesWritable bw = intermediateOI.getPrimitiveWritableObject(partial);
        HLL other = HLL.fromBytes(bw.copyBytes());
        hllbuf.hll.union(other);
      }
      
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      HLLBuffer hllbuf = (HLLBuffer)agg;
      List<Object> ret = Lists.newArrayList();
      ret.add(new LongWritable(hllbuf.hll.cardinality()));
      ret.add(new BytesWritable(hllbuf.hll.toBytes()));
      return ret;
    }
    
  }
  
  static class HLLBuffer implements AggregationBuffer {
    public HLL hll;
    public HLLBuffer(){
      hll = new HLL(14/*log2m*/, 5/*registerWidth*/);
    }
  }
  
  static enum HLLMode {
    HASHING, MERGING
  }
}
