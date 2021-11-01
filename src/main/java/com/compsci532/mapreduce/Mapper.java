package com.compsci532.mapreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public interface Mapper  {
    void map(String key, String value, MapResultWriter writer) throws IOException;

}
