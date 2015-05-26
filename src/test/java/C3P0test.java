import com.gome.bigdata.dao.C3P0Factory;
import com.gome.bigdata.dao.ConnectionFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by lujia on 2015/5/25.
 */
public class C3P0test {

    public static void main(String[] args) {

        System.out.println("aa");


        try {
            Connection conn = ConnectionFactory.getConnection();
//            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();

            String sql = "select * from BDJG150.GOME_LOGIN_INFO";
            System.out.println(stmt.execute(sql));
            conn.commit();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
