package com.nus.cool.core.schema;

public enum CompressType {

    /**
     * Compress type for finger print of data store
     * in HashMetaField.
     */
    KeyFinger,

    /**
     * Compress type for data store in HashMetaField.
     */
    KeyString,

    /**
     * Compress type for keys of HashField
     */
    KeyHash,

    /**
     * Compress type for values of hashField
     */
    Value,

    /**
     * Compress type for values of rangeField
     */
    ValueFast

}
