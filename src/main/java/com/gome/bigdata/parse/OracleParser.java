package com.gome.bigdata.parse;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.gome.bigdata.attr.OracleAttr;
import com.gome.rt.chain.attr.HBaseAttr;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created by lujia on 2015/3/19.
 */
public class OracleParser {
    private static Logger log = Logger.getLogger(OracleParser.class.getName());

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


    /**
     * 生成update操作的 sql json
     *
     * @param optJson
     * @param mapping
     * @return
     */
    public static String jsonToUpdatePutJson(JSONObject optJson, JSONObject mapping) {
        JSONObject putJson = new JSONObject();
        String hiveTableName = getHiveTableNameFromOpt(optJson);
        putJson.put("table", hiveTableName);
        putJson.put("opt", HBaseAttr.UPDATE);

        JSONObject property = optJson.getJSONObject("META-FILEDVALUE");
        if (property.containsKey("ID_before")) {
            //todo 之后添加update
            log.error(String.format("UPDATE ID, opt: %s", optJson.toJSONString()));
            property.remove("ID_before");
//            return null;
        }

        String family = HBaseAttr.FAMILY_NAME;
        String rowKey = getRowKeyFromOpt(hiveTableName, mapping, property);
        if (rowKey == null) {
            return null;
        }

        Put put = new Put(Bytes.toBytes(rowKey));
        String hbaseCol = "";
        for (String key : property.keySet()) {
            hbaseCol = mapping.getJSONObject(hiveTableName).getJSONObject("COLS").getString(key);
            String val = property.getString(key);
            if (val == null) {
                continue;
            }
            put.add(Bytes.toBytes(family), Bytes.toBytes(hbaseCol), Bytes.toBytes(val));
        }
        putJson.put("put", put);

        return putJson;

    }

    /**
     * 生成 insert操作的 sql json
     *
     * @param optJson
     * @param mapping
     * @return
     */
    public static JSONObject jsonToInsertPutJson(JSONObject optJson, JSONObject mapping) {
        JSONObject putJson = new JSONObject();
        String hiveTableName = getHiveTableNameFromOpt(optJson);
        putJson.put("table", hiveTableName);
        putJson.put("opt", HBaseAttr.INSERT);

        JSONObject property = optJson.getJSONObject("META-FILEDVALUE");
        String family = HBaseAttr.FAMILY_NAME;
        String rowKey = getRowKeyFromOpt(hiveTableName, mapping, property);
        if (rowKey == null) {
            return null;
        }

        Put put = new Put(Bytes.toBytes(rowKey));
        String hbaseCol = "";
        for (String key : property.keySet()) {
            hbaseCol = mapping.getJSONObject(hiveTableName).getJSONObject("COLS").getString(key);
            String val = property.getString(key);
            if (val == null) {
                continue;
            }
            put.add(Bytes.toBytes(family), Bytes.toBytes(hbaseCol), Bytes.toBytes(val));
        }
        putJson.put("put", put);

        return putJson;
    }

    /**
     * 生成delete操作的 sql json
     *
     * @param optJson
     * @return
     */
    public static JSONObject jsonToDeleteJson(JSONObject optJson, JSONObject mapping) {
        JSONObject deleteJson = new JSONObject();
        String hiveTableName = getHiveTableNameFromOpt(optJson);
        deleteJson.put("table", hiveTableName);
        deleteJson.put("opt", HBaseAttr.DELETE);

        JSONObject property = optJson.getJSONObject("META-FILEDVALUE");
        String family = HBaseAttr.FAMILY_NAME;
        String rowKey = getRowKeyFromOpt(hiveTableName, mapping, property);
        if (rowKey == null) {
            return null;
        }

        Delete delete = new Delete(Bytes.toBytes(rowKey));

        deleteJson.put("delete", delete);

        return deleteJson;
    }
}
