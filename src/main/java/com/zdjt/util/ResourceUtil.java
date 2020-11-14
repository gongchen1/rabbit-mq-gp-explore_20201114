package com.zdjt.util;

import java.util.ResourceBundle;

/**
 * @Author: qingshan
 * @Description: 咕泡学院，只为更好的你
 * 配置文件读取工具类
 */
public class ResourceUtil {
    private static final ResourceBundle resourceBundle;

    static{
        resourceBundle = ResourceBundle.getBundle("config");
    }

    public static String getKey(String key){
        return resourceBundle.getString(key);
    }

}
