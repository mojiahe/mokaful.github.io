---
title: Hive UDAF函数使用
date: 2019-07-25 15:26:02
categories:
- Technology
tags:
- Hive
---

## 1.什么是UDAF
UDF只能实现一进一出的操作，如果需要实现多进一出，则需要实现UDAF。比如： Hive查询数据时，有些聚类函数在HQL没有自带，需要用户自定义实现； 用户自定义聚合函数: Sum, Average

## 2.实现UFAF的步骤
引入如下两下类：
```java
import org.apache.hadoop.hive.ql.exec.UDAF  
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator  
```

- 首先需要继承org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver，并实现org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2接口。
- 然后构造GenericUDAFEvaluator类，实现MapReduce的计算过程，其中有3个关键的方
	- iterate：获取mapper，输送去做merge，相当于map函数
	- merge：combiner合并mapper，相当于partition阶段
	- terminate：合并所有combiner返回结果，相当于reduce阶段

- 最后再实现一个继承AbstractGenericUDAFResolver的类，重载其getEvaluator的方法，返回一个GenericUDAFEvaluator的实例

## 3.实例（Collect_set函数）
```java
/**
 * GenericUDAFCollectSet
 */
@Description(name = "collect_set", value = "_FUNC_(x) - Returns a set of objects with duplicate elements eliminated")
public class GenericUDAFCollectSet extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(GenericUDAFCollectSet.class.getName());

    public GenericUDAFCollectSet() {
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        //判别参数个数
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }
        //判别是否是基本类型，可以重写成支持复合类型
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + parameters[0].getTypeName() + " was passed as parameter 1.");
        }
        //指定调用的Evaluator,用来接收消息和指定UDAF如何调用
        return new GenericUDAFMkSetEvaluator();
    }

    public static class GenericUDAFMkSetEvaluator extends GenericUDAFEvaluator {

        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private PrimitiveObjectInspector inputOI;
        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
        // of objs)
        private StandardListObjectInspector loi;

        private StandardListObjectInspector internalMergeOI;

        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            // init output object inspectors
            // The output of a partial aggregation is a list
            /**
             * collect_set函数每个阶段分析
             * 1.PARTIAL1阶段，原始数据到部分聚合，在collect_set中，则是将原始数据放入set中，所以，
             * 输入数据类型是PrimitiveObjectInspector，输出类型是StandardListObjectInspector
             * 2.在其他情况，有两种情形：（1）两个set之间的数据合并，也就是不满足if条件情况下
             *（2）直接从原始数据到set，这种情况的出现是为了兼容从原始数据直接到set，也就是说map后
             * 直接到输出，没有reduce过程，也就是COMPLETE阶段
             */
            if (m == Mode.PARTIAL1) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
                return ObjectInspectorFactory
                        .getStandardListObjectInspector((PrimitiveObjectInspector) ObjectInspectorUtils
                                .getStandardObjectInspector(inputOI));
            } else {
                //COMPLETE 阶段
                if (!(parameters[0] instanceof StandardListObjectInspector)) {
                    //no map aggregation.
                    inputOI = (PrimitiveObjectInspector)  ObjectInspectorUtils
                            .getStandardObjectInspector(parameters[0]);
                    return (StandardListObjectInspector) ObjectInspectorFactory
                            .getStandardListObjectInspector(inputOI);
                } else { //PARTIAL2,FINAL阶段，两个阶段都是list与list合并，调用一致
                    internalMergeOI = (StandardListObjectInspector) parameters[0];
                    inputOI = (PrimitiveObjectInspector) internalMergeOI.getListElementObjectInspector();
                    loi = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
                    return loi;
                }
            }
        }

        static class MkArrayAggregationBuffer implements AggregationBuffer {
            Set<Object> container;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((MkArrayAggregationBuffer) agg).container = new HashSet<Object>();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MkArrayAggregationBuffer ret = new MkArrayAggregationBuffer();
            reset(ret);
            return ret;
        }

        //mapside，将原始值转换添加到集合中
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            assert (parameters.length == 1);
            Object p = parameters[0];

            if (p != null) {
                MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
                putIntoSet(p, myagg);
            }
        }

        //mapside，临时聚集
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
            ArrayList<Object> ret = new ArrayList<Object>(myagg.container.size());
            ret.addAll(myagg.container);
            return ret;
        }
        //terminatePartial的临时聚集跟另一个聚集合并
        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
            ArrayList<Object> partialResult = (ArrayList<Object>) internalMergeOI.getList(partial);
            for(Object i : partialResult) {
                putIntoSet(i, myagg);
            }
        }

        //合并最终结果到结果集返回
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
            ArrayList<Object> ret = new ArrayList<Object>(myagg.container.size());
            ret.addAll(myagg.container);
            return ret;
        }

        private void putIntoSet(Object p, MkArrayAggregationBuffer myagg) {
            if (myagg.container.contains(p))
                return;
            Object pCopy = ObjectInspectorUtils.copyToStandardObject(p,
                    this.inputOI);
            myagg.container.add(pCopy);
        }
    }

}
```

## 4. Hive中使用UDAF
- 将java文件编译成udaf_avg.jar，进入hive客户端添加jar包
```shell
hive>add jar /home/hadoop/udaf_avg.jar
```

- 创建临时函数
```sql
hive>create temporary function udaf_avg 'hive.udaf.Avg'
```

- 查询语句
```sql
hive>select udaf_avg(people.age) from people
```

- 销毁临时函数
```sql
hive>drop temporary function udaf_avg
```