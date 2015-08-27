import com.alibaba.fastjson.JSONObject;
import com.gome.bigdata.attr.OracleAttr;
import com.gome.bigdata.parse.OracleParser;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lujia on 2015/6/16.
 */
public class ParseTest {
    public static void main(String[] args) {
//        String batchOpt = "{\"total\":{\"1\":{\"META-PRIMARYKEY\":\"id\",\"META-FILEDVALUE\":{\"id\":\"1\",\"name\":\"cc\"},\"META-PERATEIONTYPE\":\"UPDATE_FIELDCOMP\",\"META-TABLE\":\"test\",\"META-OWNER\":\"DRG_CORE_OGG\",\"META-DATABASE\":\"bigdata\"}}}";

        //String batchOpt = "{\"total\":{\"1\":{\"META-PRIMARYKEY\":\"order_num,tie_num,part_num,vendor_code,bin,batch_id,buid,item_line_num\",\"META-OWNER\":\"DRG_CORE_OGG\",\"META-DATABASE\":\"drg_3pp\",\"META-FILEDVALUE\":{\"item_line_num_BEFORE\":\"1\",\"buid\":\"8270\",\"buid_BEFORE\":\"0\",\"item_line_num\":\"1\",\"bin_BEFORE\":\"P4POD\",\"vendor_code\":\"11003367\",\"batch_id\":\"2296183397\",\"tie_num\":\"2142632485\",\"part_num_BEFORE\":\"3000000241\",\"part_num\":\"3000000241\",\"vendor_code_BEFORE\":\"11003367\",\"order_num\":\"2296183397\",\"order_num_BEFORE\":\"2296183397\",\"bin\":\"P4POD\",\"tie_num_BEFORE\":\"2142632485\",\"batch_id_BEFORE\":\"2296183397\"},\"META-PERATEIONTYPE\":\"UPDATE_FIELDCOMP_PK\",\"META-TABLE\":\"ord_fulfillment_detail\"}}}";

        String batchOpt = "{\"total\":{\"1\":{\"META-PRIMARYKEY\":\"order_num,tie_num,part_num,vendor_code,bin,batch_id,buid,item_line_num\",\"META-FILEDVALUE\":{\"vendor_code\":\"80000741\",\"batch_id\":\"3605154\",\"tie_num\":\"40949037\",\"part_num\":\"8001006718\",\"order_num\":\"2153670032\",\"buid\":\"8270\",\"vendor_loc\":\"GV\\\"L\",\"bin\":\"P4POD\",\"item_line_num\":\"1\"},\"META-PERATEIONTYPE\":\"UPDATE_FIELDCOMP\",\"META-TABLE\":\"ord_fulfillment_detail\",\"META-OWNER\":\"DRG_CORE_UAT\",\"META-DATABASE\":\"drg_npop_uat\"}}}";
        ArrayList<JSONObject> optLists = OracleParser.parseOperations(batchOpt);

        for (JSONObject opt : optLists) {
            String optType = opt.getString(OracleAttr.PERATEIONTYPE);

            String optSql = null;
            String optOwner = opt.getString(OracleAttr.OWNER);
            if (OracleAttr.CHANGE_OWNER != null) {
                optOwner = OracleAttr.CHANGE_OWNER;
            }
            StringBuilder optTableBuilder = new StringBuilder(optOwner);
            String optTable = optTableBuilder.append(".").append(opt.getString(OracleAttr.TABLE).toUpperCase()).toString();
            String optPK = opt.getString(OracleAttr.PRIMARYKEY);
            JSONObject optFiledValue = opt.getJSONObject(OracleAttr.FILEDVALUE);
            List<List> filedList = OracleParser.getFiledListHaveFilter(optFiledValue, optTable);
            List<List> primarykeyList = OracleParser.getPrimaryKeyListHaveFilter(optFiledValue, optPK, optType, optTable);
            if (OracleAttr.UPDATE.equalsIgnoreCase(optType)) {
                optSql = OracleParser.jsonToUpdateOrUpdatePkSql(filedList, primarykeyList, optTable);
            } else if (OracleAttr.INSERT.equalsIgnoreCase(optType)) {
                optSql = OracleParser.jsonToInsertSql(filedList, optTable);
            } else if (OracleAttr.DELETE.equalsIgnoreCase(optType)) {
                optSql = OracleParser.jsonToDeleteSql(primarykeyList, optTable);
            } else if (OracleAttr.UPDATEPK.equalsIgnoreCase(optType)) {
                //todo 更新PK
                optSql = OracleParser.jsonToUpdateOrUpdatePkSql(filedList, primarykeyList, optTable);
                System.out.println("Ucaccepted operation: update PK\n" + opt.toJSONString());
//                continue;
            } else {
                System.out.println("Unaccepted operation:\n" + opt.toJSONString());
                continue;
            }

            JSONObject optSqlJson = new JSONObject();
            optSqlJson.put("opt", optType);
            optSqlJson.put("table", optTable);
            optSqlJson.put("sql", optSql);

            System.out.println("Final sql: " + optSqlJson.toJSONString());
        }
    }

}
