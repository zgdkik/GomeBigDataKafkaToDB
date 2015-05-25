package com.gome.bigdata.attr;

import scala.util.parsing.combinator.testing.Str;

/**
 * Created by lujia on 2015/5/22.
 */
public class OracleAttr {

    public static String FILEDVALUE = "META-FILEDVALUE";
    public static String PRIMARYKEY = "META-PRIMARYKEY";
    public static String DATABASE = "META-DATABASE";
    public static String OWNER = "META-OWNER";
    public static String PERATEIONTYPE = "META-PERATEIONTYPE";
    public static String TABLE = "META-TABLE";


    public static String SSO_ERROR_TABLE = "SSOUSER_OGG.GOME_USER";
    public static String SSO_ERROR_COLUMN = "USER_ID";
    public static String SSO_CORRECT_COLUMN = "ID";


    public static String CHANGE_OWNER = "DBJG150";

    public static int ORACLE_BATCH_NUM = 10;

    /**
     * 插入操作
     */
    public static String INSERT = "INSERT";
    /**
     * update操作
     */
    public static String UPDATE = "UPDATE_FIELDCOMP";
    /**
     * update pk操作
     */
    public static String UPDATEPK = "UPDATE_FIELDCOMP_PK";
    /**
     * delete操作
     */
    public static String DELETE = "DELETE";
}
