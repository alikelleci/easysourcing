package io.github.alikelleci.easysourcing.core.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AnnotationUtils {

  /**
   * Find a specific annotation on a class or its hierarchy, including interfaces and meta-annotations.
   *
   * @param clazz           the class to search
   * @param annotationClass the annotation type to look for
   * @return the annotation if found, or null if not found
   */
  public static <A extends Annotation> A findAnnotation(Class<?> clazz, Class<A> annotationClass) {
    if (clazz == null || annotationClass == null) {
      return null;
    }

    Set<Class<?>> visited = new HashSet<>();
    while (clazz != null && visited.add(clazz)) {
      // Check directly for the annotation
      A annotation = clazz.getAnnotation(annotationClass);
      if (annotation != null) {
        return annotation;
      }

      // Check meta-annotations
      for (Annotation declaredAnnotation : clazz.getDeclaredAnnotations()) {
        annotation = declaredAnnotation.annotationType().getAnnotation(annotationClass);
        if (annotation != null) {
          return annotation;
        }
      }

      // Check interfaces
      for (Class<?> iface : clazz.getInterfaces()) {
        annotation = findAnnotation(iface, annotationClass);
        if (annotation != null) {
          return annotation;
        }
      }

      // Move up the class hierarchy
      clazz = clazz.getSuperclass();
    }

    return null;
  }

  /**
   * Find a specific annotation on a method, including meta-annotations.
   *
   * @param method          the method to search
   * @param annotationClass the annotation type to look for
   * @return the annotation if found, or null if not found
   */
  public static <A extends Annotation> A findAnnotation(Method method, Class<A> annotationClass) {
    if (method == null || annotationClass == null) {
      return null;
    }

    // Check directly for the annotation
    A annotation = method.getAnnotation(annotationClass);
    if (annotation != null) {
      return annotation;
    }

    // Check meta-annotations
    for (Annotation declaredAnnotation : method.getDeclaredAnnotations()) {
      annotation = declaredAnnotation.annotationType().getAnnotation(annotationClass);
      if (annotation != null) {
        return annotation;
      }
    }

    return null;
  }

  /**
   * Find all methods in a class hierarchy annotated with a specific annotation.
   *
   * @param clazz           the class to search
   * @param annotationClass the annotation type to look for
   * @return a list of methods annotated with the specified annotation
   */
  public static <A extends Annotation> List<Method> findAnnotatedMethods(Class<?> clazz, Class<A> annotationClass) {
    if (clazz == null || annotationClass == null) {
      return List.of();
    }

    Set<Method> methods = new HashSet<>();
    Set<Class<?>> visited = new HashSet<>();

    while (clazz != null && visited.add(clazz)) {
      // Check all methods in the current class
      for (Method method : clazz.getDeclaredMethods()) {
        if (isAnnotatedWith(method, annotationClass)) {
          methods.add(method);
        }
      }

      // Check interfaces
      for (Class<?> iface : clazz.getInterfaces()) {
        methods.addAll(findAnnotatedMethods(iface, annotationClass));
      }

      // Move up the class hierarchy
      clazz = clazz.getSuperclass();
    }

    return new ArrayList<>(methods);
  }

  /**
   * Helper to check if a method is annotated with a specific annotation, including meta-annotations.
   */
  private static <A extends Annotation> boolean isAnnotatedWith(Method method, Class<A> annotationClass) {
    if (method.isAnnotationPresent(annotationClass)) {
      return true;
    }

    // Check meta-annotations
    for (Annotation declaredAnnotation : method.getDeclaredAnnotations()) {
      if (declaredAnnotation.annotationType().isAnnotationPresent(annotationClass)) {
        return true;
      }
    }

    return false;
  }
}
