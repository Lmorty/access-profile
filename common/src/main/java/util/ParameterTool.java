package util;

import org.apache.commons.lang3.math.NumberUtils;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by huangxianglei
 *
 * 参数工具类
 */
public class ParameterTool extends AbstractParameterTool{
    /****************************** copy from flink util ********************************************/
    private final Map<String, String> data;


    private ParameterTool(Map<String, String> data) {
        this.data = Collections.unmodifiableMap(new HashMap<>(data));

        this.defaultData = new ConcurrentHashMap<>(data.size());

        this.unrequestedParameters =
                Collections.newSetFromMap(new ConcurrentHashMap<>(data.size()));

        unrequestedParameters.addAll(data.keySet());
    }


    /**
     * Returns {@link ParameterTool} for the given arguments. The arguments are keys followed by
     * values. Keys have to start with '-' or '--'
     *
     * <p><strong>Example arguments:</strong> --key1 value1 --key2 value2 -key3 value3
     *
     * @param args Input array arguments
     * @return A {@link ParameterTool}
     */
    public static ParameterTool fromArgs(String[] args) {
        final Map<String, String> map = new HashMap<>(args.length / 2);

        int i = 0;
        while (i < args.length) {
            final String key = getKeyFromArgs(args, i);

            if (key.isEmpty()) {
                throw new IllegalArgumentException(
                        "The input " + Arrays.toString(args) + " contains an empty argument");
            }

            i += 1; // try to find the value

            if (i >= args.length) {
                map.put(key, NO_VALUE_KEY);
            } else if (NumberUtils.isNumber(args[i])) {
                map.put(key, args[i]);
                i += 1;
            } else if (args[i].startsWith("--") || args[i].startsWith("-")) {
                // the argument cannot be a negative number because we checked earlier
                // -> the next argument is a parameter name
                map.put(key, NO_VALUE_KEY);
            } else {
                map.put(key, args[i]);
                i += 1;
            }
        }

        return fromMap(map);
    }


    /**
     * Returns {@link ParameterTool} for the given map.
     *
     * @param map A map of arguments. Both Key and Value have to be Strings
     * @return A {@link ParameterTool}
     */
    public static ParameterTool fromMap(Map<String, String> map) {
        Preconditions.checkNotNull(map, "Unable to initialize from empty map");
        return new ParameterTool(map);
    }



    /**
     * Get the key from the given args. Keys have to start with '-' or '--'. For example, --key1
     * value1 -key2 value2.
     *
     * @param args all given args.
     * @param index the index of args to be parsed.
     * @return the key of the given arg.
     */
    private static String getKeyFromArgs(String[] args, int index) {
        String key;
        if (args[index].startsWith("--")) {
            key = args[index].substring(2);
        } else if (args[index].startsWith("-")) {
            key = args[index].substring(1);
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Error parsing arguments '%s' on '%s'. Please prefix keys with -- or -.",
                            Arrays.toString(args), args[index]));
        }

        if (key.isEmpty()) {
            throw new IllegalArgumentException(
                    "The input " + Arrays.toString(args) + " contains an empty argument");
        }
        return key;
    }


    /** Returns number of parameters in {@link ParameterTool}. */
    @Override
    public int getNumberOfParameters() {
        return data.size();
    }

    /**
     * Returns the String value for the given key. If the key does not exist it will return null.
     */
    @Override
    public String get(String key) {
        addToDefaults(key, null);
        unrequestedParameters.remove(key);
        return data.get(key);
    }

    /** Check if value is set. */
    @Override
    public boolean has(String value) {
        addToDefaults(value, null);
        unrequestedParameters.remove(value);
        return data.containsKey(value);
    }

    public void addToDefaults(String key, String value) {
        final String currentValue = defaultData.get(key);
        if (currentValue == null) {
            if (value == null) {
                value = DEFAULT_UNDEFINED;
            }
            defaultData.put(key, value);
        } else {
            // there is already an entry for this key. Check if the value is the undefined
            if (currentValue.equals(DEFAULT_UNDEFINED) && value != null) {
                // update key with better default value
                defaultData.put(key, value);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParameterTool that = (ParameterTool) o;
        return Objects.equals(data, that.data)
                && Objects.equals(defaultData, that.defaultData)
                && Objects.equals(unrequestedParameters, that.unrequestedParameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, defaultData, unrequestedParameters);
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return new ParameterTool(this.data);
    }

    @Override
    public Map<String, String> toMap() {
        return data;
    }


    /****************************** copy from flink util ********************************************/




    /****************************** 木衍工具类方法 ********************************************/

    /**
     * 获取参数
     *
     * @param args
     * @param parameterEnum
     * @return
     */
}