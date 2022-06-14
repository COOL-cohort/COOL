package com.nus.cool.core.cohort.refactor;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * Partial of a tuple in project, 
 * when generated, the layout of schemas is fixed.
 * Instance of ProjectTuple can load Object[] to reuse this memory
 */
public class ProjectedTuple {
    
    private Object[] tuple;

    private HashMap<String, Integer> schema2Index;

    public Object getValueBySchema(String schema){
        if (!schema2Index.containsKey(schema)) return null;
        return tuple[schema2Index.get(schema)];
    }

    public ProjectedTuple(List<String> schemaList){
        int idx = 0;
        for(String schema : schemaList){
            this.schema2Index.put(schema, idx++);
        }
    }

    /**
     * guarante the value in load tuple is ordered according to schemas' order.
     * @param tuple
     */
    public void loadTuple(Object[] tuple){
        this.tuple = tuple;
    }

    /**
     * 
     * @return the layout of this ProjectedTuple
     */
    public String[] getSchemaList(){
        String[] ret = new String[schema2Index.size()];
        for(Entry<String, Integer> entry:schema2Index.entrySet()){
            ret[entry.getValue()] = entry.getKey();
        }
        return ret;
    }
}
