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
        String batchOpt = "{\"total\":{\"1\":{\"META-PRIMARYKEY\":\"id\",\"META-FILEDVALUE\":{\"id\":\"1\",\"name\":\"cc\"},\"META-PERATEIONTYPE\":\"UPDATE_FIELDCOMP\",\"META-TABLE\":\"test\",\"META-OWNER\":\"DRG_CORE_OGG\",\"META-DATABASE\":\"bigdata\"}}}";

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
                optSql = OracleParser.jsonToUpdateSql(filedList, primarykeyList, optTable);
            } else if (OracleAttr.INSERT.equalsIgnoreCase(optType)) {
                optSql = OracleParser.jsonToInsertSql(filedList, optTable);
            } else if (OracleAttr.DELETE.equalsIgnoreCase(optType)) {
                optSql = OracleParser.jsonToDeleteSql(primarykeyList, optTable);
            } else if (OracleAttr.UPDATEPK.equalsIgnoreCase(optType)) {
                //todo 更新PK
                System.out.println("Ucaccepted operation: update PK\n" + opt.toJSONString());
                continue;
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
