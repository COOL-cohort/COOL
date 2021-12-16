package com.nus.cool.core.cohort.filter;

import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.util.ArrayUtil;

import java.util.BitSet;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class SetFieldFilter implements FieldFilter{

    private List<String> values;

    private boolean isAll;

    private int[] cubeIDs;

    private BitSet filter;

    private InputVector chunkValues;

    public SetFieldFilter(List<String> values) {
        this.values = checkNotNull(values);
        this.isAll = this.values.contains("ALL");
        this.cubeIDs = this.isAll ? new int[2] : new int[values.size()];
    }

    @Override
    public int getMinKey() {
        return ArrayUtil.min(this.cubeIDs);
    }

    @Override
    public int getMaxKey() {
        return ArrayUtil.max(this.cubeIDs);
    }

    @Override
    public boolean accept(MetaFieldRS metaField) {
        if (this.isAll) {
            this.cubeIDs[1] = metaField.count() - 1;
            return true;
        }
        boolean bHit = false;
        int i = 0;
        for (String v : this.values) {
            int tmp = metaField.find(v);
            cubeIDs[i++] = tmp;
            bHit |= (tmp >= 0);
        }
        return bHit || (this.values.isEmpty());
    }

    @Override
    public boolean accept(FieldRS field) {
        if (this.isAll)
            return true;

        InputVector keyVec = field.getKeyVector();
        this.filter = new BitSet(keyVec.size());
        this.chunkValues = field.getValueVector();

        boolean bHit = false;
        for (int cubeId : this.cubeIDs) {
            if (cubeId >= 0) {
                int tmp = keyVec.find(cubeId);
                bHit |= (tmp >= 0);
                if (tmp >= 0)
                    this.filter.set(tmp);
            }
        }
        return bHit || (this.values.isEmpty());
    }

    @Override
    public boolean accept(int v) {
        if (this.isAll)
            return true;
        return this.filter.get(v);
    }

    @Override
    public List<String> getValues() {
        return this.values;
    }
}
