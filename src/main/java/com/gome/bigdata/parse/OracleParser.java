package com.gome.bigdata.parse;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.gome.bigdata.attr.OracleAttr;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by lujia on 2015/3/19.
 */
public class OracleParser {
    private static Logger log = Logger.getLogger(OracleParser.class.getName());

    public static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 把原始json解析成单行操作，按照顺序保存
     *
     * @param msg
     * @return 操作顺序
     */
    public static ArrayList<JSONObject> parseOperations(String msg) {
        ArrayList<JSONObject> jsonList = new ArrayList<JSONObject>();
        String obj = "";
        if (msg.trim().startsWith("[")) {
            obj = msg.trim().substring(1, msg.length() - 1);
        } else {
            obj = msg.trim();
        }

        JSONObject jsonTotal = JSON.parseObject(obj);
        JSONObject content = jsonTotal.getJSONObject("total");

        //todo 跟增涛确定是否一定是从"1"开始的
        for (int i = 1; i < content.keySet().size() + 1; i++) {
            jsonList.add(content.getJSONObject(String.valueOf(i)));
        }
        return jsonList;
    }


    /**
     * 检查每步操作的表名和字段名是否都存在
     * todo zengtao的json发生变化时一定要检查这个方法
     *
     * @param opt 记录操作的json
     * @return 是否可以执行该操作
     */
    public static boolean checkTable(JSONObject opt, JSONObject mapping) {
        try {
            boolean res = true;
            String hiveTableName = getHiveTableNameFromOpt(opt);
            //如果没有这个表就返回false
            if (!mapping.containsKey(hiveTableName)) {
                res = false;
                log.error(String.format("Table name check failure:\n table name:%s\n json:%s\n", hiveTableName, opt.toJSONString()));
                return res;
            }

            //检查 主键
            String optPk = opt.getString("META-PRIMARYKEY");
            JSONObject tableObj = mapping.getJSONObject(hiveTableName);
            JSONArray pk = tableObj.getJSONArray("PK");
            if (pk.size() == 1) {
                String hivePk = pk.getString(0);

                if (!optPk.equalsIgnoreCase(hivePk)) {
                    res = false;
                    log.error(String.format("Primary key check failure:\n table name:%s\n json:%s\n", hiveTableName, opt.toJSONString()));
                    return res;
                }
            } else {
                HashSet<String> pkSet = new HashSet<String>();
                for (int i = 0; i < pk.size(); i++) {
                    pkSet.add(pk.getString(i));
                }

                String[] keys = optPk.split(",");
                for (int i = 0; i < keys.length; i++) {
                    if (!pkSet.contains(keys[i])) {
                        res = false;
                        log.error(String.format("Primary key check failure:\n table name:%s\n json:%s\n", hiveTableName, opt.toJSONString()));
                        return res;
                    }
                }
            }


            JSONObject property = opt.getJSONObject("META-FILEDVALUE");
            if (property.containsKey("ID_before")) {
                //todo 之后添加update
//            log.error(String.format("UPDATE ID, opt: %s", opt.toJSONString()));
                property.remove("ID_before");
            }
            //检查字段
            JSONObject table = mapping.getJSONObject(hiveTableName);
            for (String col : property.keySet()) {
                if (!table.getJSONObject("COLS").keySet().contains(col)) {
                    res = false;
                    log.warn(String.format("META-FILEDVALUE check warn:\n table name:%s\n column:%s , column is not in mapping , json:%s\n", hiveTableName, col, opt.toJSONString()));
                    continue;
                }
            }
            return res;
        } catch (Exception e) {
            log.error("check table error!");
            return false;
        }

    }

    /**
     * 从操作json中获取hbase的表名
     *
     * @param opt
     * @return hbase/hive表名
     */
    public static String getHiveTableNameFromOpt(JSONObject opt) {
        StringBuffer tableName = new StringBuffer();
        String database = opt.getString("META-DATABASE");
        if ("ATGSYSDB".equalsIgnoreCase(database)) {
            tableName.append("ATGSYSDB2").append("_0_").append(opt.getString("META-OWNER")).append("_0_").append(opt.getString("META-TABLE"));
        } else {
            tableName.append(opt.getString("META-DATABASE")).append("_0_").append(opt.getString("META-OWNER")).append("_0_").append(opt.getString("META-TABLE"));
        }
        return tableName.toString();
    }

    /**
     * 从操作的json中获取rowkey
     *
     * @param tableName
     * @param mapping
     * @param attribute META-FILEDVALUE, 保存了需要修改的字段
     * @return rowKey
     */
    public static String getRowKeyFromOpt(String tableName, JSONObject mapping, JSONObject attribute) {
        StringBuffer rowKey = new StringBuffer();

        JSONObject tableObj = mapping.getJSONObject(tableName);
        JSONArray pk = tableObj.getJSONArray("PK");
        for (int i = 0; i < pk.size(); i++) {
            rowKey.append(attribute.getString(pk.getString(i)));
            if (i + 1 < pk.size()) {
                rowKey.append("_");
            }
        }
        return rowKey.toString();
    }

    private static String getBeforeRowKey(String tableName, JSONObject mapping, JSONObject attribute) {
        StringBuffer rowKey = new StringBuffer();

        JSONObject tableObj = mapping.getJSONObject(tableName);
        JSONArray pk = tableObj.getJSONArray("PK");
        for (int i = 0; i < pk.size(); i++) {
            rowKey.append(attribute.getString(pk.getString(i) + "_BEFORE"));
            if (i + 1 < pk.size()) {
                rowKey.append("_");
            }
        }
        return rowKey.toString();
    }

    /**
     * 生成 updatePK的 put
     *
     * @param optJson
     * @param mapping
     * @return
     */
    public static JSONObject jsonToUpdatePKPutJson(JSONObject optJson, JSONObject mapping) {

        JSONObject updatePKJson = new JSONObject();
        JSONObject property = optJson.getJSONObject("META-FILEDVALUE");
        String hiveTableName = getHiveTableNameFromOpt(optJson);
        updatePKJson.put("table", hiveTableName);
        updatePKJson.put("opt", OracleAttr.UPDATEPK);
        String rowKeyBefore = getBeforeRowKey(hiveTableName, mapping, property);
        updatePKJson.put("rowkey_before", rowKeyBefore);
        String rowKeyNew = getRowKeyFromOpt(hiveTableName, mapping, property);
        if (rowKeyNew == null) {
            return null;
        }
        updatePKJson.put("rowkey", rowKeyNew);
        JSONArray primaryKeys = mapping.getJSONObject(hiveTableName).getJSONArray("PK");
        updatePKJson.put("pks", primaryKeys);
        updatePKJson.put("property", property);

        return updatePKJson;
    }


    public static List<List> getFiledListHaveFilter(JSONObject jsonObjectFiledValue, String table) {
        List<List> list = new ArrayList<List>();
        int i = 1;
        for (String key : jsonObjectFiledValue.keySet()) {
            String value = jsonObjectFiledValue.getString(key);
            if (table.equals(OracleAttr.SSO_ERROR_TABLE) && key.equals(OracleAttr.SSO_ERROR_COLUMN)) {
                key = OracleAttr.SSO_CORRECT_COLUMN;
            }
            if (!key.endsWith("_BEFORE")) {
                List listFiled = new ArrayList();
                listFiled.add(i);
                listFiled.add(new StringBuffer("\"").append(key.toUpperCase()).append("\"").toString());
                if (StringUtils.isEmpty(value) || value.equals("null")) {
                    listFiled.add(null);
                } else {
                    listFiled.add(value);
                }
                list.add(listFiled);
                i++;
            }
        }
        return list;
    }

    public static List<List> getPrimaryKeyListHaveFilter(JSONObject jsonObjectFiledValue,
                                                         String primarykey, String opeate, String table) {
        String[] primarykeyArray = primarykey.split(",");
        List<List> list = new ArrayList<List>();
        int i = 1;
        for (String key : primarykeyArray) {
            List listPrimarykey = new ArrayList();
            String value = null;
            if (opeate.equals("UPDATE_FIELDCOMP_PK")) {
                value = jsonObjectFiledValue.get(key + "_BEFORE").toString();
            } else {
//                log.info(jsonObjectFiledValue.toJSONString());
//                log.info("Keys : " + primarykey + " , key :" + key);
                value = jsonObjectFiledValue.get(key).toString();
            }

            listPrimarykey.add(i);
            if (table.equals(OracleAttr.SSO_ERROR_TABLE) && key.equals(OracleAttr.SSO_ERROR_COLUMN)) {
                listPrimarykey.add("\"" + OracleAttr.SSO_CORRECT_COLUMN.toUpperCase() + "\"");
            } else {
                listPrimarykey.add("\"" + key.toUpperCase() + "\"");
            }
            listPrimarykey.add(value);
            list.add(listPrimarykey);
            i++;
        }
        return list;
    }

    public static boolean isMatcher(String regex, String string) {
        String result = "";
        try {
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(string);
            if (matcher.find()) {
                result = matcher.group();
            }
        } catch (Exception e) {
            log.error(e.getLocalizedMessage());
        }
        if (StringUtils.isEmpty(result)) {
            return false;
        }
        return true;
    }

    public static String formatDate(String dateString) {
        Date date = null;
        try {
            date = df.parse(dateString);
        } catch (ParseException e) {
            log.error(e.getLocalizedMessage());
        }
        return df.format(date);
    }

    public static String valueFormat(Object value) {
        String formatValume = "";
        if (value == null) {
            formatValume = null;
        } else if (isMatcher("^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$", value
                .toString().trim())) {
            formatValume = formatDate(value.toString().trim());
            formatValume = "TO_DATE(" + "'" + formatValume + "'," + "'YYYY-MM-DD HH24:MI:SS')";

        } else if (value.toString().contains("'")) {
            formatValume = "'" + value.toString().replace("'", "''") + "'";
        } else {
            formatValume = "'" + value + "'";
        }
        return formatValume;

    }


    /**
     * 生成update操作的 sql
     *
     * @param filedList      操作list
     * @param primarykeyList 主键list
     * @param tableName
     * @return update sql
     */
    public static String jsonToUpdateSql(List<List> filedList, List<List> primarykeyList, String tableName) {

        String sql = "";
        String filedPart = "";
        String primaryKeyPart = "";
        for (int i = 0; i < filedList.size(); i++) {
            List list = filedList.get(i);
            String filed = list.get(1).toString();
            String valueFormat = valueFormat(list.get(2));
            if (filedList.size() != i + 1) {
                filedPart = filedPart + filed + "=" + valueFormat + ",";
            } else {
                filedPart = filedPart + filed + "=" + valueFormat;
            }
        }

        for (int i = 0; i < primarykeyList.size(); i++) {
            List list = primarykeyList.get(i);
            String filed = list.get(1).toString();
            String valueFormat = valueFormat(list.get(2));
            if (primarykeyList.size() != i + 1) {
                primaryKeyPart = primaryKeyPart + filed + "=" + valueFormat + " AND ";
            } else {
                primaryKeyPart = primaryKeyPart + filed + "=" + valueFormat + "";
            }
        }
        sql = "UPDATE " + tableName + " SET " + filedPart + " WHERE " + primaryKeyPart;
        return sql;
    }

    /**
     * 生成 insert操作的 sql
     *
     * @param filedList 操作的数据list
     * @param tableName
     * @return insert sql
     */
    public static String jsonToInsertSql(List<List> filedList, String tableName) {
        String sql = "";
        String filedPart = "(";
        String valePart = "(";
        for (int i = 0; i < filedList.size(); i++) {
            List list = filedList.get(i);
            String filed = list.get(1).toString();
            String valueFormat = valueFormat(list.get(2));
            if (filedList.size() != i + 1) {
                filedPart = filedPart + filed + ",";
                valePart = valePart + valueFormat + ",";
            } else {
                filedPart = filedPart + filed + ")";
                valePart = valePart + valueFormat + ")";
            }
        }
        sql = "INSERT INTO " + tableName + filedPart + " VALUES" + valePart;
        return sql;
    }

    /**
     * 生成delete操作的 sql
     *
     * @param primarykeyList
     * @param table
     * @return
     */
    public static String jsonToDeleteSql(List<List> primarykeyList, String table) {

        String sql = "";
        String primaryKeyPart = "";
        for (int i = 0; i < primarykeyList.size(); i++) {
            List list = (List) primarykeyList.get(i);
            if (primarykeyList.size() != i + 1) {
                primaryKeyPart = primaryKeyPart + list.get(1) + "='" + list.get(2) + "' AND ";
            } else {
                primaryKeyPart = primaryKeyPart + list.get(1) + "='" + list.get(2) + "'";
            }
        }

        sql = " DELETE FROM " + table + " WHERE " + primaryKeyPart;
        return sql;
    }
}
