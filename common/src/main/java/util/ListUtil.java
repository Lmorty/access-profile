package util;

import java.util.List;

public class ListUtil {

    public static <T>T getIfExists(List<T> list, int index){
        if(index < 0 || null == list || index > list.size() - 1){
            return null;
        }else{
            return list.get(index);
        }
    }
}
