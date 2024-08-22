package org.apache.rocketmq.filter.parser;

public interface SelectorParserConstants {

    /**
     * RegularExpression Id.
     */
    int NOT = 8;

    /**
     * RegularExpression Id.
     */
    int AND = 9;

    /**
     * RegularExpression Id.
     */
    int OR = 10;

    /**
     * RegularExpression Id.
     */
    int BETWEEN = 11;

    /**
     * RegularExpression Id.
     */
    int IN = 12;

    /**
     * RegularExpression Id.
     */
    int TRUE = 13;

    /**
     * RegularExpression Id.
     */
    int FALSE = 14;

    /**
     * RegularExpression Id.
     */
    int NULL = 15;

    /**
     * RegularExpression Id.
     */
    int IS = 16;

    /**
     * RegularExpression Id.
     */
    int CONTAINS = 17;

    /**
     * RegularExpression Id.
     */
    int STARTSWITH = 18;

    /**
     * RegularExpression Id.
     */
    int ENDSWITH = 19;

    /**
     * RegularExpression Id.
     */
    int DECIMAL_LITERAL = 20;

    /**
     * RegularExpression Id.
     */
    int FLOATING_POINT_LITERAL = 21;

    /**
     * RegularExpression Id.
     */
    int STRING_LITERAL = 23;

    /**
     * RegularExpression Id.
     */
    int ID = 24;

    /**
     * Literal token values.
     */
    String[] TOKEN_IMAGE = {
            "<EOF>",
            "\" \"",
            "\"\\t\"",
            "\"\\n\"",
            "\"\\r\"",
            "\"\\f\"",
            "<LINE_COMMENT>",
            "<BLOCK_COMMENT>",
            "\"NOT\"",
            "\"AND\"",
            "\"OR\"",
            "\"BETWEEN\"",
            "\"IN\"",
            "\"TRUE\"",
            "\"FALSE\"",
            "\"NULL\"",
            "\"IS\"",
            "\"CONTAINS\"",
            "\"STARTSWITH\"",
            "\"ENDSWITH\"",
            "<DECIMAL_LITERAL>",
            "<FLOATING_POINT_LITERAL>",
            "<EXPONENT>",
            "<STRING_LITERAL>",
            "<ID>",
            "\"=\"",
            "\"<>\"",
            "\">\"",
            "\">=\"",
            "\"<\"",
            "\"<=\"",
            "\"(\"",
            "\",\"",
            "\")\"",
            "\"+\"",
            "\"-\"",
    };

}