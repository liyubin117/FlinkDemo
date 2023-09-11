//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.lyb.classloader;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.flink.util.*;
import org.apache.flink.util.UserClassLoaderJarTestUtils;
import org.assertj.core.api.Assertions;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class FlinkUserCodeClassLoadersTest extends TestLogger {
    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    public static final String USER_CLASS = "UserClass";
    public static final String USER_CLASS_CODE = "import java.io.Serializable;\npublic class UserClass implements Serializable {}";
    private static File userJar;

    public FlinkUserCodeClassLoadersTest() {
    }

    @BeforeClass
    public static void prepare() throws Exception {
        userJar =
                UserClassLoaderJarTestUtils.createJarFile(
                        temporaryFolder.newFolder("test-jar"),
                        "test-classloader.jar",
                        USER_CLASS,
                        USER_CLASS_CODE);
    }

    @Test
    public void testParentFirstClassLoading() throws Exception {
        ClassLoader parentClassLoader = this.getClass().getClassLoader();
        URL childCodePath = this.getClass().getProtectionDomain().getCodeSource().getLocation();
        URLClassLoader childClassLoader1 = createParentFirstClassLoader(childCodePath, parentClassLoader);
        URLClassLoader childClassLoader2 = createParentFirstClassLoader(childCodePath, parentClassLoader);
        String className = FlinkUserCodeClassLoadersTest.class.getName();
        Class<?> clazz1 = Class.forName(className, false, parentClassLoader);
        Class<?> clazz2 = Class.forName(className, false, childClassLoader1);
        Class<?> clazz3 = Class.forName(className, false, childClassLoader2);
        Assert.assertEquals(clazz1, clazz2);
        Assert.assertEquals(clazz1, clazz3);
        childClassLoader1.close();
        childClassLoader2.close();
    }

    @Test
    public void testChildFirstClassLoading() throws Exception {
        ClassLoader parentClassLoader = this.getClass().getClassLoader();
        URL childCodePath = this.getClass().getProtectionDomain().getCodeSource().getLocation();
        URLClassLoader childClassLoader1 = createChildFirstClassLoader(childCodePath, parentClassLoader);
        URLClassLoader childClassLoader2 = createChildFirstClassLoader(childCodePath, parentClassLoader);
        String className = FlinkUserCodeClassLoadersTest.class.getName();
        Class<?> clazz1 = Class.forName(className, false, parentClassLoader);
        Class<?> clazz2 = Class.forName(className, false, childClassLoader1);
        Class<?> clazz3 = Class.forName(className, false, childClassLoader2);
        Assert.assertNotEquals(clazz1, clazz2);
        Assert.assertNotEquals(clazz1, clazz3);
        Assert.assertNotEquals(clazz2, clazz3);
        childClassLoader1.close();
        childClassLoader2.close();
    }

    @Test
    public void testRepeatedChildFirstClassLoading() throws Exception {
        ClassLoader parentClassLoader = this.getClass().getClassLoader();
        URL childCodePath = this.getClass().getProtectionDomain().getCodeSource().getLocation();
        URLClassLoader childClassLoader = createChildFirstClassLoader(childCodePath, parentClassLoader);
        String className = FlinkUserCodeClassLoadersTest.class.getName();
        Class<?> clazz1 = Class.forName(className, false, parentClassLoader);
        Class<?> clazz2 = Class.forName(className, false, childClassLoader);
        Class<?> clazz3 = Class.forName(className, false, childClassLoader);
        Class<?> clazz4 = Class.forName(className, false, childClassLoader);
        Assert.assertNotEquals(clazz1, clazz2);
        Assert.assertEquals(clazz2, clazz3);
        Assert.assertEquals(clazz2, clazz4);
        childClassLoader.close();
    }

    @Test
    public void testRepeatedParentFirstPatternClass() throws Exception {
        String className = FlinkUserCodeClassLoadersTest.class.getName();
        String parentFirstPattern = className.substring(0, className.lastIndexOf(46));
        ClassLoader parentClassLoader = this.getClass().getClassLoader();
        URL childCodePath = this.getClass().getProtectionDomain().getCodeSource().getLocation();
        URLClassLoader childClassLoader = FlinkUserCodeClassLoaders.childFirst(new URL[]{childCodePath}, parentClassLoader, new String[]{parentFirstPattern}, FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER, true);
        Class<?> clazz1 = Class.forName(className, false, parentClassLoader);
        Class<?> clazz2 = Class.forName(className, false, childClassLoader);
        Class<?> clazz3 = Class.forName(className, false, childClassLoader);
        Class<?> clazz4 = Class.forName(className, false, childClassLoader);
        Assert.assertEquals(clazz1, clazz2);
        Assert.assertEquals(clazz1, clazz3);
        Assert.assertEquals(clazz1, clazz4);
        childClassLoader.close();
    }

    @Test
    public void testGetClassLoaderInfo() throws Exception {
        ClassLoader parentClassLoader = this.getClass().getClassLoader();
        URL childCodePath = this.getClass().getProtectionDomain().getCodeSource().getLocation();
        URLClassLoader childClassLoader = createChildFirstClassLoader(childCodePath, parentClassLoader);
        String formattedURL = ClassLoaderUtil.formatURL(childCodePath);
        Assert.assertEquals(ClassLoaderUtil.getUserCodeClassLoaderInfo(childClassLoader), "URL ClassLoader:" + formattedURL);
        childClassLoader.close();
    }

    @Test
    public void testGetClassLoaderInfoWithClassLoaderClosed() throws Exception {
        ClassLoader parentClassLoader = this.getClass().getClassLoader();
        URL childCodePath = this.getClass().getProtectionDomain().getCodeSource().getLocation();
        URLClassLoader childClassLoader = createChildFirstClassLoader(childCodePath, parentClassLoader);
        childClassLoader.close();
        Assert.assertThat(ClassLoaderUtil.getUserCodeClassLoaderInfo(childClassLoader), Matchers.startsWith("Cannot access classloader info due to an exception."));
    }

    private static MutableURLClassLoader createParentFirstClassLoader(URL childCodePath, ClassLoader parentClassLoader) {
        return FlinkUserCodeClassLoaders.parentFirst(new URL[]{childCodePath}, parentClassLoader, FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER, true);
    }

    private static MutableURLClassLoader createChildFirstClassLoader(URL childCodePath, ClassLoader parentClassLoader) {
        return FlinkUserCodeClassLoaders.childFirst(new URL[]{childCodePath}, parentClassLoader, new String[0], FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER, true);
    }

    @Test
    public void testClosingOfClassloader() throws Exception {
        String className = ClassToLoad.class.getName();
        ClassLoader parentClassLoader = ClassLoader.getSystemClassLoader().getParent();
        URL childCodePath = this.getClass().getProtectionDomain().getCodeSource().getLocation();
        URLClassLoader childClassLoader = createChildFirstClassLoader(childCodePath, parentClassLoader);
        Class<?> loadedClass = childClassLoader.loadClass(className);
        Assert.assertNotSame(ClassToLoad.class, loadedClass);
        childClassLoader.close();
        this.expectedException.expect(CoreMatchers.isA(IllegalStateException.class));
        childClassLoader.loadClass(className);
    }

    @Test
    public void testParentFirstClassLoadingByAddURL() throws Exception {
        URL childCodePath = this.getClass().getProtectionDomain().getCodeSource().getLocation();
        MutableURLClassLoader parentClassLoader = createChildFirstClassLoader(childCodePath, this.getClass().getClassLoader());
        MutableURLClassLoader childClassLoader1 = createParentFirstClassLoader(childCodePath, parentClassLoader);
        MutableURLClassLoader childClassLoader2 = createParentFirstClassLoader(childCodePath, parentClassLoader);
        this.assertClassNotFoundException(USER_CLASS, false, parentClassLoader);
        this.assertClassNotFoundException(USER_CLASS, false, childClassLoader1);
        this.assertClassNotFoundException(USER_CLASS, false, childClassLoader2);
        parentClassLoader.addURL(userJar.toURI().toURL());
        Class<?> clazz1 = Class.forName(USER_CLASS, false, parentClassLoader);
        Class<?> clazz2 = Class.forName(USER_CLASS, false, childClassLoader1);
        Class<?> clazz3 = Class.forName(USER_CLASS, false, childClassLoader2);
        Assert.assertEquals(clazz1, clazz2);
        Assert.assertEquals(clazz1, clazz3);
        parentClassLoader.close();
        childClassLoader1.close();
        childClassLoader2.close();
    }

    @Test
    public void testChildFirstClassLoadingByAddURL() throws Exception {
        URL childCodePath = this.getClass().getProtectionDomain().getCodeSource().getLocation();
        MutableURLClassLoader parentClassLoader = createChildFirstClassLoader(childCodePath, this.getClass().getClassLoader());
        MutableURLClassLoader childClassLoader1 = createChildFirstClassLoader(childCodePath, parentClassLoader);
        MutableURLClassLoader childClassLoader2 = createChildFirstClassLoader(childCodePath, parentClassLoader);
        this.assertClassNotFoundException(USER_CLASS, false, parentClassLoader);
        this.assertClassNotFoundException(USER_CLASS, false, childClassLoader1);
        this.assertClassNotFoundException(USER_CLASS, false, childClassLoader2);
        URL userJarURL = userJar.toURI().toURL();
        childClassLoader1.addURL(userJarURL);
        childClassLoader2.addURL(userJarURL);
        this.assertClassNotFoundException(USER_CLASS, false, parentClassLoader);
        Class<?> clazz1 = Class.forName(USER_CLASS, false, childClassLoader1);
        Class<?> clazz2 = Class.forName(USER_CLASS, false, childClassLoader2);
        Assert.assertNotEquals(clazz1, clazz2);
        parentClassLoader.close();
        childClassLoader1.close();
        childClassLoader2.close();
    }

    private void assertClassNotFoundException(String className, boolean initialize, ClassLoader classLoader) {
        try {
            Class.forName(className, initialize, classLoader);
            Assertions.fail("Should fail.");
        } catch (ClassNotFoundException var5) {
        }

    }

    private static class ClassToLoad {
        private ClassToLoad() {
        }
    }
}
