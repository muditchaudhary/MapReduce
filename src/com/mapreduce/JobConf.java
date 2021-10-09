package com.mapreduce;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class JobConf {
    public String JobMasterID;
    public String JobMasterJob;
    public Class<? extends Mapper> MapFunc;
    public Object ReduceFunc;


    public JobConf(String JobMasterID, String JobMasterJob){
        JobMasterID = JobMasterID;
        JobMasterJob = JobMasterJob;
    }

    public void setMapper(Class <? extends Mapper> MyMapFunc) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        MapFunc = MyMapFunc;
    }

    public void setReducer (Class <? extends Reducer> ReduceFunc){
        ReduceFunc = ReduceFunc;
    }

}
