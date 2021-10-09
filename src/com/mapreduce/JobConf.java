package com.mapreduce;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class JobConf {
    public String JobMasterID;
    public String JobMasterJob;
    public Class<? extends Mapper> MapFunc;
    public Class<? extends Reducer> ReduceFunc;


    public JobConf(String JobMasterID, String JobMasterJob){
        this.JobMasterID = JobMasterID;
        this.JobMasterJob = JobMasterJob;
    }

    public void setMapper(Class <? extends Mapper> MyMapFunc) {
        this.MapFunc = MyMapFunc;
    }

    public void setReducer (Class <? extends Reducer> ReduceFunc){
        this.ReduceFunc = ReduceFunc;
    }

}
