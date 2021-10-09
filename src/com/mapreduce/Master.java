package com.mapreduce;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.UUID;

public class Master {
    String MasterID;
    JobConf jobConfig;

    public Master(String MasterID){
        this.MasterID = MasterID;
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

    public Object createMapper() throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        Class <?> MapperCls = this.jobConfig.MapFunc;
        String uuid = UUID.randomUUID().toString();
        Object thisMapper = MapperCls.getConstructors()[0].newInstance(uuid);
        return thisMapper;
    }

    public Object createReducer() throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        Class <?> ReducerCls = this.jobConfig.ReduceFunc;
        String uuid = UUID.randomUUID().toString();
        Object test = ReducerCls.getConstructors()[0];
        Object thisReducer = ReducerCls.getConstructors()[0].newInstance(uuid);
        return thisReducer;
    }

}
