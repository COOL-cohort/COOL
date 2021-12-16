package com.nus.cool.core.util.converter;

public class StringIntConverter implements NumericConverter {
    @Override
    public int toInt(String v) {
        return Integer.parseInt(v);
    }

    @Override
    public String getString(int i) {
        return String.valueOf(i);
    }
}
