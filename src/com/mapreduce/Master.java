package com.mapreduce;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Random;
import java.util.UUID;

public class Master {
    String MasterID;

    public Master(String MasterID){
        MasterID = MasterID;
    }

    public void runJob(JobConf jobConfig) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class<?> MapCls = jobConfig.MapFunc.getClass();
        Method method = MapCls.getMethod("Map");
        method.invoke(jobConfig.MapFunc);
    }

    public void createMapper(JobConf jobConfig) throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        Class <?> MapperCls = jobConfig.MapFunc;
        byte[] array = new byte[7]; // length is bounded by 7
        new Random().nextBytes(array);
        String uuid = UUID.randomUUID().toString();
        Object ob = MapperCls.getConstructors()[0].newInstance(uuid);
        Method method = MapperCls.getMethod("Map");
        method.invoke(ob);
    }

}
