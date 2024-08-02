package org.apache.rocketmq.common.utils;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class ServiceProvider {

    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private static ClassLoader thisClassLoader;
    
    public static final String PREFIX = "META-INF/service/";
    
    static {
        thisClassLoader = getClassLoader();
    }
    
    protected static String objectId(Object o) {
        if (o == null) {
            return "null";
        } else {
            return o.getClass().getName() + "@" + System.identityHashCode(o);
        }
    }

    protected static ClassLoader getClassLoader() {
        try {
            return ServiceProvider.class.getClassLoader();
        } catch (SecurityException e) {
            LOG.error("Unable to get classloader for class {} due to security restrictions , error info {}", ServiceProvider.class, e.getMessage());
            throw e;
        }
    }
    
    protected static ClassLoader getContextClassLoader() {
        ClassLoader classLoader = null;
        try {
            classLoader = Thread.currentThread().getContextClassLoader();
        } catch (SecurityException ignored) {
        }
        return classLoader;
    }
    
    protected static InputStream getResourceAsStream(ClassLoader loader, String name) {
        if (loader != null) {
            return loader.getResourceAsStream(name);
        } else {
            return ClassLoader.getSystemResourceAsStream(name);
        }
    }
    
    public static <T> List<T> load(Class<?> clazz) {
        String fullName = PREFIX + clazz.getName();
        return load(fullName, clazz);
    }
    
    public static <T> List<T> load(String name, Class<?> clazz) {
        LOG.info("Looking for a resource file of name [{}] ...", name);
        List<T> services = new ArrayList<>();
        InputStream is = getResourceAsStream(getContextClassLoader(), name);
        if (is == null) {
            LOG.warn("No resource file with name [{}] found.", name);
            return services;
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String serviceName = reader.readLine();
            List<String> names = new ArrayList<>();
            while (serviceName != null && !serviceName.isEmpty()) {
                LOG.info("Creating an instance as specified by file {} which was present in the path of the context classloader.", name);
                if (!names.contains(serviceName)) {
                    names.add(serviceName);
                    services.add(initService(getContextClassLoader(), serviceName, clazz));
                }
                serviceName = reader.readLine();
            }
        } catch (Exception e) {
            LOG.error("Error occurred when looking for resource file " + name, e);
        }
        return services;
    }
    
    public static <T> T loadClass(Class<?> clazz) {
        String fullName = PREFIX + clazz.getName();
        return loadClass(fullName, clazz);
    }
    
    public static <T> T loadClass(String name, Class<?> clazz) {
        LOG.info("Looking for a resource file of name [{}] ...", name);
        T s = null;
        InputStream is = getResourceAsStream(getContextClassLoader(), name);
        if (is == null) {
            LOG.warn("No resource file with name [{}] found.", name);
            return null;
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String serviceName = reader.readLine();
            if (serviceName != null && !serviceName.isEmpty()) {
                s = initService(getContextClassLoader(), serviceName, clazz);
            } else {
                LOG.warn("ServiceName is empty!");
            }
        } catch (Exception e) {
            LOG.warn("Error occurred when looking for resource file " + name, e);
        }
        return s;
    }

    protected static <T> T initService(ClassLoader classLoader, String serviceName, Class<?> clazz) {
        Class<?> serviceClazz = null;
        try {
            if (classLoader != null) {
                try {
                    serviceClazz = classLoader.loadClass(serviceName);
                    if (clazz.isAssignableFrom(serviceClazz)) {
                        LOG.info("Loaded class {} from classloader {}", serviceClazz.getName(), objectId(classLoader));
                    } else {
                        LOG.error("Class {} loaded from classloader {} does not extend {} as loaded by this classloader.", serviceClazz.getName(), objectId(serviceClazz.getClassLoader()), clazz.getName());
                    }
                    return (T) serviceClazz.getDeclaredConstructor().newInstance();
                } catch (ClassNotFoundException ex) {
                    if (classLoader == thisClassLoader) {
                        LOG.warn("Unable to locate any class {} via classloader {}", serviceName, objectId(classLoader));
                        throw ex;
                    }
                } catch (NoClassDefFoundError e) {
                    if (classLoader == thisClassLoader) {
                        LOG.warn("Class {} cannot be loaded via classloader {}.it depends on some other class that cannot be found.", serviceClazz, objectId(classLoader));
                        throw e;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Unable to init service.", e);
        }
        return (T) serviceClazz;
    }

}