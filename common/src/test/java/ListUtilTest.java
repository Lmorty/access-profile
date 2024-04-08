import org.junit.Test;
import util.ListUtil;

import java.util.ArrayList;

public class ListUtilTest {

    @Test
    public void testFunction(){
        ArrayList<Integer> strings = new ArrayList<>();

        strings.add(1);
        strings.add(2);

        Integer element1 = ListUtil.getIfExists(strings, 0);
        Integer element2 = ListUtil.getIfExists(strings, 1);
        Integer element3 = ListUtil.getIfExists(strings, 2);

        System.out.println(element1);
        System.out.println(element2);
        System.out.println(element3);
    }
}
