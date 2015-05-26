import com.gome.bigdata.utils.StringUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lujia on 2015/5/26.
 */
public class ListTest {

    public static void main(String[] args) {
        List<String> ll = new ArrayList<String>();

        for (int i = 0; i < 10; i++) {
            ll.add(String.valueOf(i));
        }

        for (int i = 0; i < ll.size(); i++) {
            String a = ll.get(i);
            System.out.println(a);
            System.out.println(ll.size());
        }
    }
}
