package azkaban.execapp;

import org.apache.log4j.Hierarchy;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerRepository;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Hashtable;

public class Log4jHelper {

    private static final Logger LOGGER = Logger.getLogger(Log4jHelper.class);

    /**
     * 对于需要回收的Logger，需要清除Hierarchy.ht里面的缓存
     */
    public static void closeLogger(Logger log) {
        if(log == null) {
            return;
        }

        try {
            LoggerRepository loggerRepository = log.getLoggerRepository();
            if(loggerRepository instanceof Hierarchy) {
                Hierarchy hierarchy = (Hierarchy) loggerRepository;
                Class<?> cls = hierarchy.getClass();
                Field field = cls.getDeclaredField("ht");
                field.setAccessible(true);

                Hashtable ht = (Hashtable) field.get(hierarchy);

                Class<?> clsCategoryKey = Class.forName("org.apache.log4j.CategoryKey");
                Constructor<?> constructor = clsCategoryKey.getDeclaredConstructor(String.class);
                constructor.setAccessible(true);
                constructor.newInstance(log.getName());

                ht.remove(constructor.newInstance(log.getName()));
            }
        } catch (Exception e) {
            LOGGER.error("Log4jHelper closeLogger error", e);
        }
    }
}