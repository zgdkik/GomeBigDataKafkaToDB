import com.gome.bigdata.dao.ConnectionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class dbctest {
    public static void main(String[] args) {
        boolean flag = false;
        // 在此处成具体的数据库验证

        // 声明一个数据库操作对象
        PreparedStatement pstmt = null;
        // 声明一个结果集对象
        ResultSet rs = null;
        // 声明一个SQL变量，用于保存SQL语句
        String sql = null;
        // DataBaseConnection为具体的数据库连接及关闭操作类
        Connection con = null;
        System.out.println("111111111111111");
        // 连接数据库
        con = ConnectionFactory.getConnection();
        System.out.println("222222222222222");

        // 编写SQL语句
        sql = "SELECT * FROM BDJG150.GOME_LOGIN_INFO WHERE user_id=?";
        try {
            // 实例化数据库操作对象
            pstmt = con.prepareStatement(sql);

            System.out.println("操作对象已被实例化");

            // 设置pstmt的内容，是按ID和密码验证
            pstmt.setString(1, "limeng");
//            pstmt.setString(2, "limeng");

            System.out.println("获得username,password");

            // 查询记录
            rs = pstmt.executeQuery();
            System.out.println("执行查询完毕");
            // 判断是否有记录
            if (rs.next()) {
                // 如果有记录，则执行此段代码
                // 用户是合法的，可以登陆
                flag = true;

                System.out.println("用户合法");
            }
            // 依次关闭

            rs.close();
            pstmt.close();

        } catch (Exception e) {
            System.out.println(e);
        } finally {
            // 最后一定要保证数据库已被关闭
            try {
                con.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}

