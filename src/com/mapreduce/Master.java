package com.mapreduce;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.UUID;

public class Master {
    String masterID;
    JobConf jobConfig;


    public Master(){
        this.masterID = UUID.randomUUID().toString();
    }

    public void setJobConfig(JobConf jobConfig){
        this.jobConfig = jobConfig;
    }

    public void runJob() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException {
        Object mapper = createMapper();
        Method mapperMethod = this.jobConfig.MapFunc.getMethod("Map");
        mapperMethod.invoke(mapper);

        Object reducer = createReducer();
        Method reducerMethod = this.jobConfig.ReduceFunc.getMethod("Reduce");
        reducerMethod.invoke(reducer);
    }

    private Object createMapper() throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        Class <?> MapperCls = this.jobConfig.MapFunc;
        Object thisMapper = MapperCls.newInstance();
        return thisMapper;
    }

    private Object createReducer() throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        Class <?> ReducerCls = this.jobConfig.ReduceFunc;
        Object thisReducer = ReducerCls.newInstance();
        return thisReducer;
    }

}
