package com.nucypher.kafka.utils;

import com.nucypher.crypto.AlgorithmName;
import com.nucypher.crypto.EncryptionAlgorithm;
import com.nucypher.kafka.errors.CommonException;
import org.reflections.Reflections;

import java.util.HashSet;
import java.util.Set;

/**
 * Utils for working with {@link EncryptionAlgorithm} implementations
 */
public class EncryptionAlgorithmUtils {

    private static final String PACKAGE =
            EncryptionAlgorithm.class.getPackage().getName();

    /**
     * @return all available algorithms and their names
     */
    public static Set<String> getAvailableAlgorithms() {
        Set<String> algorithms = new HashSet<>();
        Reflections reflections = new Reflections(PACKAGE);
        Set<Class<? extends EncryptionAlgorithm>> classes =
                reflections.getSubTypesOf(EncryptionAlgorithm.class);
        for (Class<? extends EncryptionAlgorithm> clazz : classes) {
            AlgorithmName algorithmName = clazz.getAnnotation(AlgorithmName.class);
            String name = algorithmName != null ?
                    algorithmName.value() : clazz.getCanonicalName();
            algorithms.add(name);
        }
        return algorithms;
    }

    /**
     * Get instance of {@link EncryptionAlgorithm}
     *
     * @param name algorithm name or full class name
     * @return instance of {@link EncryptionAlgorithm}
     */
    public static EncryptionAlgorithm getEncryptionAlgorithm(String name) {
        String nameLowerCase = name.toLowerCase();
        Reflections reflections = new Reflections(PACKAGE);
        Set<Class<? extends EncryptionAlgorithm>> classes =
                reflections.getSubTypesOf(EncryptionAlgorithm.class);
        Class<? extends EncryptionAlgorithm> algorithmClass = null;
        for (Class<? extends EncryptionAlgorithm> clazz : classes) {
            AlgorithmName algorithmName = clazz.getAnnotation(AlgorithmName.class);
            if (algorithmName != null &&
                    algorithmName.value().toLowerCase().equals(nameLowerCase) ||
                    clazz.getCanonicalName().equals(name)) {
                algorithmClass = clazz;
                break;
            }
        }
        if (algorithmClass == null) {
            throw new CommonException("Algorithm '%s' not found", name);
        }
        try {
            return algorithmClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new CommonException(e);
        }
    }

    /**
     * Get instance of {@link EncryptionAlgorithm} by class name
     *
     * @param className full class name
     * @return instance of {@link EncryptionAlgorithm}
     */
    @SuppressWarnings("unchecked")
    public static EncryptionAlgorithm getEncryptionAlgorithmByClass(String className) {
        try {
            Class<? extends EncryptionAlgorithm> algorithmClass =
                    (Class<? extends EncryptionAlgorithm>) Class.forName(className);
            return algorithmClass.newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new CommonException(e);
        }
    }

    /**
     * Get instance of {@link EncryptionAlgorithm} by class
     *
     * @param algorithmClass class of encryption algorithm
     * @return instance of {@link EncryptionAlgorithm}
     */
    public static EncryptionAlgorithm getEncryptionAlgorithmByClass(
            Class<? extends EncryptionAlgorithm> algorithmClass) {
        try {
            return algorithmClass.newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new CommonException(e);
        }
    }
}
