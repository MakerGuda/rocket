package org.apache.rocketmq.filter.parser;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.rocketmq.filter.expression.*;

import java.io.StringReader;
import java.util.ArrayList;

public class SelectorParser implements SelectorParserConstants {

    private static final Cache<String, Object> PARSE_CACHE = CacheBuilder.newBuilder().maximumSize(100).build();

    static private int[] jjLa10;

    static private int[] jjLa11;

    static {
        jj_la1_init_0();
        jj_la1_init_1();
    }

    final private int[] jjLa1 = new int[16];

    final private JJCalls[] jj2Rtns = new JJCalls[7];

    final private LookaheadSuccess jjLs = new LookaheadSuccess();

    public SelectorParserTokenManager tokenSource;

    public Token token;

    public Token jjNt;

    SimpleCharStream jjInputStream;

    private int jjNtk;

    private Token jjScanpos, jjLastpos;

    private int jjLa;

    private int jjGen;

    private boolean jjRescan = false;

    private int jjGc = 0;

    private final java.util.List<int[]> jjExpentries = new java.util.ArrayList<>();

    private int[] jjExpentry;

    private int jjKind = -1;

    private final int[] jjLasttokens = new int[100];

    private int jjEndpos;

    protected SelectorParser(String sql) {
        this(new StringReader(sql));
    }

    public SelectorParser(java.io.InputStream stream) {
        this(stream, null);
    }

    public SelectorParser(java.io.InputStream stream, String encoding) {
        try {
            jjInputStream = new SimpleCharStream(stream, encoding, 1, 1);
        } catch (java.io.UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        tokenSource = new SelectorParserTokenManager(jjInputStream);
        token = new Token();
        jjNtk = -1;
        jjGen = 0;
        for (int i = 0; i < 16; i++) jjLa1[i] = -1;
        for (int i = 0; i < jj2Rtns.length; i++) jj2Rtns[i] = new JJCalls();
    }

    public SelectorParser(java.io.Reader stream) {
        jjInputStream = new SimpleCharStream(stream, 1, 1);
        tokenSource = new SelectorParserTokenManager(jjInputStream);
        token = new Token();
        jjNtk = -1;
        jjGen = 0;
        for (int i = 0; i < 16; i++) jjLa1[i] = -1;
        for (int i = 0; i < jj2Rtns.length; i++) jj2Rtns[i] = new JJCalls();
    }

    public SelectorParser(SelectorParserTokenManager tm) {
        tokenSource = tm;
        token = new Token();
        jjNtk = -1;
        jjGen = 0;
        for (int i = 0; i < 16; i++) jjLa1[i] = -1;
        for (int i = 0; i < jj2Rtns.length; i++) jj2Rtns[i] = new JJCalls();
    }

    public static BooleanExpression parse(String sql) throws MQFilterException {
        Object result = PARSE_CACHE.getIfPresent(sql);
        if (result instanceof MQFilterException) {
            throw (MQFilterException) result;
        } else if (result instanceof BooleanExpression) {
            return (BooleanExpression) result;
        } else {
            ComparisonExpression.CONVERT_STRING_EXPRESSIONS.set(true);
            try {
                BooleanExpression e = new SelectorParser(sql).parse();
                PARSE_CACHE.put(sql, e);
                return e;
            } catch (MQFilterException t) {
                PARSE_CACHE.put(sql, t);
                throw t;
            } finally {
                ComparisonExpression.CONVERT_STRING_EXPRESSIONS.remove();
            }
        }
    }

    private static void jj_la1_init_0() {
        jjLa10 = new int[]{0x400, 0x200, 0x6010000, 0x6000000, 0x10000, 0x780e1900, 0x0, 0x0, 0x78020000, 0x40000, 0x80000, 0x800, 0x1000, 0x81b0e100, 0x81b0e000, 0xb0e000,};
    }

    private static void jj_la1_init_1() {
        jjLa11 = new int[]{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x8, 0x0, 0x0,};
    }

    protected BooleanExpression parse() throws MQFilterException {
        try {
            return this.JmsSelector();
        } catch (Throwable e) {
            throw new MQFilterException("Invalid MessageSelector. ", e);
        }
    }

    private BooleanExpression asBooleanExpression(Expression value) throws ParseException {
        if (value instanceof BooleanExpression) {
            return (BooleanExpression) value;
        }
        if (value instanceof PropertyExpression) {
            return UnaryExpression.createBooleanCast(value);
        }
        throw new ParseException("Expression will not result in a boolean value: " + value);
    }

    final public BooleanExpression JmsSelector() throws ParseException {
        Expression left;
        left = orExpression();
        return asBooleanExpression(left);
    }

    final public Expression orExpression() throws ParseException {
        Expression left;
        Expression right;
        left = andExpression();
        while (true) {
            if (((jjNtk == -1) ? jj_ntk() : jjNtk) == OR) {
                System.out.println();
            } else {
                jjLa1[0] = jjGen;
                break;
            }
            jj_consume_token(OR);
            right = andExpression();
            left = LogicExpression.createOR(asBooleanExpression(left), asBooleanExpression(right));
        }
        return left;
    }

    final public Expression andExpression() throws ParseException {
        Expression left;
        Expression right;
        left = equalityExpression();
        while (true) {
            if (((jjNtk == -1) ? jj_ntk() : jjNtk) == AND) {
                System.out.println();
            } else {
                jjLa1[1] = jjGen;
                break;
            }
            jj_consume_token(AND);
            right = equalityExpression();
            left = LogicExpression.createAND(asBooleanExpression(left), asBooleanExpression(right));
        }
        return left;
    }

    final public Expression equalityExpression() throws ParseException {
        Expression left;
        Expression right;
        left = comparisonExpression();
        label_3:
        while (true) {
            switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
                case IS:
                case 25:
                case 26:
                    break;
                default:
                    jjLa1[2] = jjGen;
                    break label_3;
            }
            switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
                case 25:
                    jj_consume_token(25);
                    right = comparisonExpression();
                    left = ComparisonExpression.createEqual(left, right);
                    break;
                case 26:
                    jj_consume_token(26);
                    right = comparisonExpression();
                    left = ComparisonExpression.createNotEqual(left, right);
                    break;
                default:
                    jjLa1[3] = jjGen;
                    if (jj_2_1()) {
                        jj_consume_token(IS);
                        jj_consume_token(NULL);
                        left = ComparisonExpression.createIsNull(left);
                    } else {
                        if (((jjNtk == -1) ? jj_ntk() : jjNtk) == IS) {
                            jj_consume_token(IS);
                            jj_consume_token(NOT);
                            jj_consume_token(NULL);
                            left = ComparisonExpression.createIsNotNull(left);
                        } else {
                            jjLa1[4] = jjGen;
                            jj_consume_token(-1);
                            throw new ParseException();
                        }
                    }
            }
        }
        return left;
    }

    final public Expression comparisonExpression() throws ParseException {
        Expression left;
        Expression right;
        Expression low;
        Expression high;
        String t;
        ArrayList list;
        left = unaryExpr();
        label_4:
        while (true) {
            switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
                case NOT:
                case BETWEEN:
                case IN:
                case CONTAINS:
                case STARTSWITH:
                case ENDSWITH:
                case 27:
                case 28:
                case 29:
                case 30:
                    break;
                default:
                    jjLa1[5] = jjGen;
                    break label_4;
            }
            switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
                case 27:
                    jj_consume_token(27);
                    right = unaryExpr();
                    left = ComparisonExpression.createGreaterThan(left, right);
                    break;
                case 28:
                    jj_consume_token(28);
                    right = unaryExpr();
                    left = ComparisonExpression.createGreaterThanEqual(left, right);
                    break;
                case 29:
                    jj_consume_token(29);
                    right = unaryExpr();
                    left = ComparisonExpression.createLessThan(left, right);
                    break;
                case 30:
                    jj_consume_token(30);
                    right = unaryExpr();
                    left = ComparisonExpression.createLessThanEqual(left, right);
                    break;
                case CONTAINS:
                    jj_consume_token(CONTAINS);
                    t = stringLitteral();
                    left = ComparisonExpression.createContains(left, t);
                    break;
                default:
                    jjLa1[8] = jjGen;
                    if (jj_2_2()) {
                        jj_consume_token(NOT);
                        jj_consume_token(CONTAINS);
                        t = stringLitteral();
                        left = ComparisonExpression.createNotContains(left, t);
                    } else {
                        if (((jjNtk == -1) ? jj_ntk() : jjNtk) == STARTSWITH) {
                            jj_consume_token(STARTSWITH);
                            t = stringLitteral();
                            left = ComparisonExpression.createStartsWith(left, t);
                        } else {
                            jjLa1[9] = jjGen;
                            if (jj_2_3()) {
                                jj_consume_token(NOT);
                                jj_consume_token(STARTSWITH);
                                t = stringLitteral();
                                left = ComparisonExpression.createNotStartsWith(left, t);
                            } else {
                                if (((jjNtk == -1) ? jj_ntk() : jjNtk) == ENDSWITH) {
                                    jj_consume_token(ENDSWITH);
                                    t = stringLitteral();
                                    left = ComparisonExpression.createEndsWith(left, t);
                                } else {
                                    jjLa1[10] = jjGen;
                                    if (jj_2_4()) {
                                        jj_consume_token(NOT);
                                        jj_consume_token(ENDSWITH);
                                        t = stringLitteral();
                                        left = ComparisonExpression.createNotEndsWith(left, t);
                                    } else {
                                        if (((jjNtk == -1) ? jj_ntk() : jjNtk) == BETWEEN) {
                                            jj_consume_token(BETWEEN);
                                            low = unaryExpr();
                                            jj_consume_token(AND);
                                            high = unaryExpr();
                                            left = ComparisonExpression.createBetween(left, low, high);
                                        } else {
                                            jjLa1[11] = jjGen;
                                            if (jj_2_5()) {
                                                jj_consume_token(NOT);
                                                jj_consume_token(BETWEEN);
                                                low = unaryExpr();
                                                jj_consume_token(AND);
                                                high = unaryExpr();
                                                left = ComparisonExpression.createNotBetween(left, low, high);
                                            } else {
                                                if (((jjNtk == -1) ? jj_ntk() : jjNtk) == IN) {
                                                    jj_consume_token(IN);
                                                    jj_consume_token(31);
                                                    t = stringLitteral();
                                                    list = new ArrayList<>();
                                                    list.add(t);
                                                    while (true) {
                                                        if (((jjNtk == -1) ? jj_ntk() : jjNtk) == 32) {
                                                            System.out.println();
                                                        } else {
                                                            jjLa1[6] = jjGen;
                                                            break;
                                                        }
                                                        jj_consume_token(32);
                                                        t = stringLitteral();
                                                        list.add(t);
                                                    }
                                                    jj_consume_token(33);
                                                    left = ComparisonExpression.createInFilter(left, list);
                                                } else {
                                                    jjLa1[12] = jjGen;
                                                    if (jj_2_6()) {
                                                        jj_consume_token(NOT);
                                                        jj_consume_token(IN);
                                                        jj_consume_token(31);
                                                        t = stringLitteral();
                                                        list = new ArrayList<>();
                                                        list.add(t);
                                                        while (true) {
                                                            if (((jjNtk == -1) ? jj_ntk() : jjNtk) == 32) {
                                                                System.out.println();
                                                            } else {
                                                                jjLa1[7] = jjGen;
                                                                break;
                                                            }
                                                            jj_consume_token(32);
                                                            t = stringLitteral();
                                                            list.add(t);
                                                        }
                                                        jj_consume_token(33);
                                                        left = ComparisonExpression.createNotInFilter(left, list);
                                                    } else {
                                                        jj_consume_token(-1);
                                                        throw new ParseException();
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
            }
        }
        return left;
    }

    final public Expression unaryExpr() throws ParseException {
        Expression left;
        if (jj_2_7()) {
            jj_consume_token(34);
            left = unaryExpr();
        } else {
            switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
                case 35:
                    jj_consume_token(35);
                    left = unaryExpr();
                    left = UnaryExpression.createNegate(left);
                    break;
                case NOT:
                    jj_consume_token(NOT);
                    left = unaryExpr();
                    left = UnaryExpression.createNOT(asBooleanExpression(left));
                    break;
                case TRUE:
                case FALSE:
                case NULL:
                case DECIMAL_LITERAL:
                case FLOATING_POINT_LITERAL:
                case STRING_LITERAL:
                case ID:
                case 31:
                    left = primaryExpr();
                    break;
                default:
                    jjLa1[13] = jjGen;
                    jj_consume_token(-1);
                    throw new ParseException();
            }
        }
        return left;
    }

    final public Expression primaryExpr() throws ParseException {
        Expression left;
        switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
            case TRUE:
            case FALSE:
            case NULL:
            case DECIMAL_LITERAL:
            case FLOATING_POINT_LITERAL:
            case STRING_LITERAL:
                left = literal();
                break;
            case ID:
                left = variable();
                break;
            case 31:
                jj_consume_token(31);
                left = orExpression();
                jj_consume_token(33);
                break;
            default:
                jjLa1[14] = jjGen;
                jj_consume_token(-1);
                throw new ParseException();
        }
        return left;
    }

    final public ConstantExpression literal() throws ParseException {
        Token t;
        String s;
        ConstantExpression left;
        switch ((jjNtk == -1) ? jj_ntk() : jjNtk) {
            case STRING_LITERAL:
                s = stringLitteral();
                left = new ConstantExpression(s);
                break;
            case DECIMAL_LITERAL:
                t = jj_consume_token(DECIMAL_LITERAL);
                left = ConstantExpression.createFromDecimal(t.image);
                break;
            case FLOATING_POINT_LITERAL:
                t = jj_consume_token(FLOATING_POINT_LITERAL);
                left = ConstantExpression.createFloat(t.image);
                break;
            case TRUE:
                jj_consume_token(TRUE);
                left = BooleanConstantExpression.TRUE;
                break;
            case FALSE:
                jj_consume_token(FALSE);
                left = BooleanConstantExpression.FALSE;
                break;
            case NULL:
                jj_consume_token(NULL);
                left = BooleanConstantExpression.NULL;
                break;
            default:
                jjLa1[15] = jjGen;
                jj_consume_token(-1);
                throw new ParseException();
        }
        return left;
    }

    final public String stringLitteral() throws ParseException {
        Token t;
        StringBuilder rc = new StringBuilder();
        t = jj_consume_token(STRING_LITERAL);
        String image = t.image;
        for (int i = 1; i < image.length() - 1; i++) {
            char c = image.charAt(i);
            if (c == '\'')
                i++;
            rc.append(c);
        }
        return rc.toString();
    }

    final public PropertyExpression variable() throws ParseException {
        Token t;
        PropertyExpression left;
        t = jj_consume_token(ID);
        left = new PropertyExpression(t.image);
        return left;
    }

    private boolean jj_2_1() {
        jjLa = 2;
        jjLastpos = jjScanpos = token;
        try {
            return !jj_3_1();
        } catch (LookaheadSuccess ls) {
            return true;
        } finally {
            jj_save(0, 2);
        }
    }

    private boolean jj_2_2() {
        jjLa = 2;
        jjLastpos = jjScanpos = token;
        try {
            return !jj_3_2();
        } catch (LookaheadSuccess ls) {
            return true;
        } finally {
            jj_save(1, 2);
        }
    }

    private boolean jj_2_3() {
        jjLa = 2;
        jjLastpos = jjScanpos = token;
        try {
            return !jj_3_3();
        } catch (LookaheadSuccess ls) {
            return true;
        } finally {
            jj_save(2, 2);
        }
    }

    private boolean jj_2_4() {
        jjLa = 2;
        jjLastpos = jjScanpos = token;
        try {
            return !jj_3_4();
        } catch (LookaheadSuccess ls) {
            return true;
        } finally {
            jj_save(3, 2);
        }
    }

    private boolean jj_2_5() {
        jjLa = 2;
        jjLastpos = jjScanpos = token;
        try {
            return !jj_3_5();
        } catch (LookaheadSuccess ls) {
            return true;
        } finally {
            jj_save(4, 2);
        }
    }

    private boolean jj_2_6() {
        jjLa = 2;
        jjLastpos = jjScanpos = token;
        try {
            return !jj_3_6();
        } catch (LookaheadSuccess ls) {
            return true;
        } finally {
            jj_save(5, 2);
        }
    }

    private boolean jj_2_7() {
        jjLa = 2147483647;
        jjLastpos = jjScanpos = token;
        try {
            return !jj_3_7();
        } catch (LookaheadSuccess ls) {
            return true;
        } finally {
            jj_save(6, 2147483647);
        }
    }

    private boolean jj_3R_34() {
        if (jj_scan_token(26)) return true;
        return jj_3R_30();
    }

    private boolean jj_3R_43() {
        if (jj_scan_token(BETWEEN)) return true;
        if (jj_3R_7()) return true;
        if (jj_scan_token(AND)) return true;
        return jj_3R_7();
    }

    private boolean jj_3R_31() {
        Token xsp;
        xsp = jjScanpos;
        if (jj_3R_33()) {
            jjScanpos = xsp;
            if (jj_3R_34()) {
                jjScanpos = xsp;
                if (jj_3_1()) {
                    jjScanpos = xsp;
                    return jj_3R_35();
                }
            }
        }
        return false;
    }

    private boolean jj_3R_33() {
        if (jj_scan_token(25)) return true;
        return jj_3R_30();
    }

    private boolean jj_3_4() {
        if (jj_scan_token(NOT)) return true;
        if (jj_scan_token(ENDSWITH)) return true;
        return jj_3R_27();
    }

    private boolean jj_3R_15() {
        if (jj_scan_token(31)) return true;
        if (jj_3R_18()) return true;
        return jj_scan_token(33);
    }

    private boolean jj_3R_14() {
        return jj_3R_17();
    }

    private boolean jj_3R_13() {
        return jj_3R_16();
    }

    private boolean jj_3R_42() {
        if (jj_scan_token(ENDSWITH)) return true;
        return jj_3R_27();
    }

    private boolean jj_3R_17() {
        return jj_scan_token(ID);
    }

    private boolean jj_3R_12() {
        Token xsp;
        xsp = jjScanpos;
        if (jj_3R_13()) {
            jjScanpos = xsp;
            if (jj_3R_14()) {
                jjScanpos = xsp;
                return jj_3R_15();
            }
        }
        return false;
    }

    private boolean jj_3R_28() {
        if (jj_3R_30()) return true;
        Token xsp;
        while (true) {
            xsp = jjScanpos;
            if (jj_3R_31()) {
                jjScanpos = xsp;
                break;
            }
        }
        return false;
    }

    private boolean jj_3_3() {
        if (jj_scan_token(NOT)) return true;
        if (jj_scan_token(STARTSWITH)) return true;
        return jj_3R_27();
    }

    private boolean jj_3R_41() {
        if (jj_scan_token(STARTSWITH)) return true;
        return jj_3R_27();
    }

    private boolean jj_3R_11() {
        return jj_3R_12();
    }

    private boolean jj_3R_29() {
        if (jj_scan_token(AND)) return true;
        return jj_3R_28();
    }

    private boolean jj_3_7() {
        if (jj_scan_token(34)) return true;
        return jj_3R_7();
    }

    private boolean jj_3_2() {
        if (jj_scan_token(NOT)) return true;
        if (jj_scan_token(CONTAINS)) return true;
        return jj_3R_27();
    }

    private boolean jj_3R_10() {
        if (jj_scan_token(NOT)) return true;
        return jj_3R_7();
    }

    private boolean jj_3R_40() {
        if (jj_scan_token(CONTAINS)) return true;
        return jj_3R_27();
    }

    private boolean jj_3R_9() {
        if (jj_scan_token(35)) return true;
        return jj_3R_7();
    }

    private boolean jj_3R_27() {
        return jj_scan_token(STRING_LITERAL);
    }

    private boolean jj_3R_25() {
        if (jj_3R_28()) return true;
        Token xsp;
        while (true) {
            xsp = jjScanpos;
            if (jj_3R_29()) {
                jjScanpos = xsp;
                break;
            }
        }
        return false;
    }

    private boolean jj_3R_8() {
        if (jj_scan_token(34)) return true;
        return jj_3R_7();
    }

    private boolean jj_3R_39() {
        if (jj_scan_token(30)) return true;
        return jj_3R_7();
    }

    private boolean jj_3R_7() {
        Token xsp;
        xsp = jjScanpos;
        if (jj_3R_8()) {
            jjScanpos = xsp;
            if (jj_3R_9()) {
                jjScanpos = xsp;
                if (jj_3R_10()) {
                    jjScanpos = xsp;
                    return jj_3R_11();
                }
            }
        }
        return false;
    }

    private boolean jj_3R_38() {
        if (jj_scan_token(29)) return true;
        return jj_3R_7();
    }

    private boolean jj_3R_46() {
        if (jj_scan_token(32)) return true;
        return jj_3R_27();
    }

    private boolean jj_3R_26() {
        if (jj_scan_token(OR)) return true;
        return jj_3R_25();
    }

    private boolean jj_3R_37() {
        if (jj_scan_token(28)) return true;
        return jj_3R_7();
    }

    private boolean jj_3R_24() {
        return jj_scan_token(NULL);
    }

    private boolean jj_3R_36() {
        if (jj_scan_token(27)) return true;
        return jj_3R_7();
    }

    private boolean jj_3R_32() {
        Token xsp;
        xsp = jjScanpos;
        if (jj_3R_36()) {
            jjScanpos = xsp;
            if (jj_3R_37()) {
                jjScanpos = xsp;
                if (jj_3R_38()) {
                    jjScanpos = xsp;
                    if (jj_3R_39()) {
                        jjScanpos = xsp;
                        if (jj_3R_40()) {
                            jjScanpos = xsp;
                            if (jj_3_2()) {
                                jjScanpos = xsp;
                                if (jj_3R_41()) {
                                    jjScanpos = xsp;
                                    if (jj_3_3()) {
                                        jjScanpos = xsp;
                                        if (jj_3R_42()) {
                                            jjScanpos = xsp;
                                            if (jj_3_4()) {
                                                jjScanpos = xsp;
                                                if (jj_3R_43()) {
                                                    jjScanpos = xsp;
                                                    if (jj_3_5()) {
                                                        jjScanpos = xsp;
                                                        if (jj_3R_44()) {
                                                            jjScanpos = xsp;
                                                            return jj_3_6();
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    private boolean jj_3R_23() {
        return jj_scan_token(FALSE);
    }

    private boolean jj_3R_18() {
        if (jj_3R_25()) return true;
        Token xsp;
        while (true) {
            xsp = jjScanpos;
            if (jj_3R_26()) {
                jjScanpos = xsp;
                break;
            }
        }
        return false;
    }

    private boolean jj_3R_22() {
        return jj_scan_token(TRUE);
    }

    private boolean jj_3_6() {
        if (jj_scan_token(NOT)) return true;
        if (jj_scan_token(IN)) return true;
        if (jj_scan_token(31)) return true;
        if (jj_3R_27()) return true;
        Token xsp;
        while (true) {
            xsp = jjScanpos;
            if (jj_3R_46()) {
                jjScanpos = xsp;
                break;
            }
        }
        return jj_scan_token(33);
    }

    private boolean jj_3R_45() {
        if (jj_scan_token(32)) return true;
        return jj_3R_27();
    }

    private boolean jj_3R_30() {
        if (jj_3R_7()) return true;
        Token xsp;
        while (true) {
            xsp = jjScanpos;
            if (jj_3R_32()) {
                jjScanpos = xsp;
                break;
            }
        }
        return false;
    }

    private boolean jj_3R_21() {
        return jj_scan_token(FLOATING_POINT_LITERAL);
    }

    private boolean jj_3R_20() {
        return jj_scan_token(DECIMAL_LITERAL);
    }

    private boolean jj_3R_35() {
        if (jj_scan_token(IS)) return true;
        if (jj_scan_token(NOT)) return true;
        return jj_scan_token(NULL);
    }

    private boolean jj_3R_44() {
        if (jj_scan_token(IN)) return true;
        if (jj_scan_token(31)) return true;
        if (jj_3R_27()) return true;
        Token xsp;
        while (true) {
            xsp = jjScanpos;
            if (jj_3R_45()) {
                jjScanpos = xsp;
                break;
            }
        }
        return jj_scan_token(33);
    }

    private boolean jj_3R_19() {
        return jj_3R_27();
    }

    private boolean jj_3_1() {
        if (jj_scan_token(IS)) return true;
        return jj_scan_token(NULL);
    }

    private boolean jj_3R_16() {
        Token xsp;
        xsp = jjScanpos;
        if (jj_3R_19()) {
            jjScanpos = xsp;
            if (jj_3R_20()) {
                jjScanpos = xsp;
                if (jj_3R_21()) {
                    jjScanpos = xsp;
                    if (jj_3R_22()) {
                        jjScanpos = xsp;
                        if (jj_3R_23()) {
                            jjScanpos = xsp;
                            return jj_3R_24();
                        }
                    }
                }
            }
        }
        return false;
    }

    private boolean jj_3_5() {
        if (jj_scan_token(NOT)) return true;
        if (jj_scan_token(BETWEEN)) return true;
        if (jj_3R_7()) return true;
        if (jj_scan_token(AND)) return true;
        return jj_3R_7();
    }

    private Token jj_consume_token(int kind) throws ParseException {
        Token oldToken;
        if ((oldToken = token).next != null) token = token.next;
        else token = token.next = tokenSource.getNextToken();
        jjNtk = -1;
        if (token.kind == kind) {
            jjGen++;
            if (++jjGc > 100) {
                jjGc = 0;
                for (JJCalls jj2Rtn : jj2Rtns) {
                    JJCalls c = jj2Rtn;
                    while (c != null) {
                        if (c.gen < jjGen) c.first = null;
                        c = c.next;
                    }
                }
            }
            return token;
        }
        token = oldToken;
        jjKind = kind;
        throw generateParseException();
    }

    private boolean jj_scan_token(int kind) {
        if (jjScanpos == jjLastpos) {
            jjLa--;
            if (jjScanpos.next == null) {
                jjLastpos = jjScanpos = jjScanpos.next = tokenSource.getNextToken();
            } else {
                jjLastpos = jjScanpos = jjScanpos.next;
            }
        } else {
            jjScanpos = jjScanpos.next;
        }
        if (jjRescan) {
            int i = 0;
            Token tok = token;
            while (tok != null && tok != jjScanpos) {
                i++;
                tok = tok.next;
            }
            if (tok != null) jj_add_error_token(kind, i);
        }
        if (jjScanpos.kind != kind) return true;
        if (jjLa == 0 && jjScanpos == jjLastpos) throw jjLs;
        return false;
    }

    private int jj_ntk() {
        if ((jjNt = token.next) == null)
            return jjNtk = (token.next = tokenSource.getNextToken()).kind;
        else
            return jjNtk = jjNt.kind;
    }

    private void jj_add_error_token(int kind, int pos) {
        if (pos >= 100) return;
        if (pos == jjEndpos + 1) {
            jjLasttokens[jjEndpos++] = kind;
        } else if (jjEndpos != 0) {
            jjExpentry = new int[jjEndpos];
            System.arraycopy(jjLasttokens, 0, jjExpentry, 0, jjEndpos);
            boolean exists = false;
            for (int[] expentry : jjExpentries) {
                exists = true;
                if (expentry.length == jjExpentry.length) {
                    for (int i = 0; i < jjExpentry.length; i++) {
                        if (expentry[i] != jjExpentry[i]) {
                            exists = false;
                            break;
                        }
                    }
                    if (exists) break;
                }
            }
            if (!exists) jjExpentries.add(jjExpentry);
            if (pos != 0) jjLasttokens[(jjEndpos = pos) - 1] = kind;
        }
    }

    /**
     * Generate ParseException.
     */
    public ParseException generateParseException() {
        jjExpentries.clear();
        boolean[] la1tokens = new boolean[36];
        if (jjKind >= 0) {
            la1tokens[jjKind] = true;
            jjKind = -1;
        }
        for (int i = 0; i < 16; i++) {
            if (jjLa1[i] == jjGen) {
                for (int j = 0; j < 32; j++) {
                    if ((jjLa10[i] & (1 << j)) != 0) {
                        la1tokens[j] = true;
                    }
                    if ((jjLa11[i] & (1 << j)) != 0) {
                        la1tokens[32 + j] = true;
                    }
                }
            }
        }
        for (int i = 0; i < 36; i++) {
            if (la1tokens[i]) {
                jjExpentry = new int[1];
                jjExpentry[0] = i;
                jjExpentries.add(jjExpentry);
            }
        }
        jjEndpos = 0;
        jj_rescan_token();
        jj_add_error_token(0, 0);
        int[][] exptokseq = new int[jjExpentries.size()][];
        for (int i = 0; i < jjExpentries.size(); i++) {
            exptokseq[i] = jjExpentries.get(i);
        }
        return new ParseException(token, exptokseq, TOKEN_IMAGE);
    }

    private void jj_rescan_token() {
        jjRescan = true;
        for (int i = 0; i < 7; i++) {
            try {
                JJCalls p = jj2Rtns[i];
                do {
                    if (p.gen > jjGen) {
                        jjLa = p.arg;
                        jjLastpos = jjScanpos = p.first;
                        switch (i) {
                            case 0:
                                jj_3_1();
                                break;
                            case 1:
                                jj_3_2();
                                break;
                            case 2:
                                jj_3_3();
                                break;
                            case 3:
                                jj_3_4();
                                break;
                            case 4:
                                jj_3_5();
                                break;
                            case 5:
                                jj_3_6();
                                break;
                            case 6:
                                jj_3_7();
                                break;
                        }
                    }
                    p = p.next;
                } while (p != null);
            } catch (LookaheadSuccess ignore) {
            }
        }
        jjRescan = false;
    }

    private void jj_save(int index, int xla) {
        JJCalls p = jj2Rtns[index];
        while (p.gen > jjGen) {
            if (p.next == null) {
                p = p.next = new JJCalls();
                break;
            }
            p = p.next;
        }
        p.gen = jjGen + xla - jjLa;
        p.first = token;
        p.arg = xla;
    }

    static private final class LookaheadSuccess extends java.lang.Error {
    }

    static final class JJCalls {
        int gen;
        Token first;
        int arg;
        JJCalls next;
    }

}