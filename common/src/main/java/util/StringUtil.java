package util;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class StringUtil {

    /**
     * 表名转驼峰
     * e.g dim_seller_all -> dimSellerAll
     * @param origin
     * @return
     */
    public static String convertIntoCamelCase(String origin,String seperator){

        String[] sar = origin.toLowerCase(Locale.ROOT).split(seperator);

        ArrayList<String> strings = new ArrayList<>();

        for (int i = 0; i < sar.length; i++) {
            if(i == 0){
                strings.add(sar[i]);
            }else{
                strings.add(upperFirstLatter(sar[i]));
            }
        }

        return StringUtils.join(strings,"");
    }

    /**
     * 首字母大写
     * @param letter
     * @return
     */
    public static String upperFirstLatter(String letter){
        char[] chars = letter.toCharArray();
        if(chars[0]>='a' && chars[0]<='z'){
            chars[0] = (char) (chars[0]-32);
        }
        return new String(chars);
    }
}
