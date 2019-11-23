package com.qihoo.qsql.codegen;

import com.github.picadoh.imc.compiler.InMemoryClassManager;
import com.github.picadoh.imc.compiler.InMemoryCompiler;
import com.github.picadoh.imc.loader.CompilationPackageLoader;
import com.github.picadoh.imc.model.CompilationPackage;
import com.github.picadoh.imc.model.CompilationUnit;
import com.github.picadoh.imc.model.JavaSourceFromString;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.qihoo.qsql.exec.Requirement;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;
import org.apache.calcite.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide class header code, and methods which can compile it as a Class in memory.
 */
public abstract class ClassBodyWrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClassBodyComposer.class);
    private static final String CLASS_NAME_PREFIX = "Requirement";
    protected String className;
    protected ClassBodyComposer composer = new ClassBodyComposer(this.getClass());

    /**
     * ClassBodyWrapper constructor, which create a new Class name.
     */
    public ClassBodyWrapper() {
        Calendar calendar = Util.calendar();
        AtomicInteger classId = new AtomicInteger(
            calendar.get(Calendar.WEEK_OF_MONTH) * 10000
                + calendar.get(Calendar.HOUR_OF_DAY) * 1000
                + calendar.get(Calendar.MINUTE) * 100
                + calendar.get(Calendar.SECOND));

        this.className = CLASS_NAME_PREFIX + classId;
    }

    /**
     * For compiling class in memory.
     *
     * @param source a complete Java Class code
     * @param name Class name
     * @return Class instance in memory
     * @throws InMemoryCompiler.CompilerException When some error occurred, compilerException will be throw
     * @throws ClassNotFoundException When class is not found in jvm, a exception will be throw
     */
    public static Class compileSourceAndLoadClass(String source, String name, String extraJars)
        throws InMemoryCompiler.CompilerException,
        ClassNotFoundException {
        WithClassPathInMemoryCompiler compiler = new WithClassPathInMemoryCompiler();
        CompilationPackage compilationPackage = compiler.singleCompile(name, source, extraJars);
        CompilationPackageLoader loader = new CompilationPackageLoader();
        Map<String, Class<?>> classes = loader.loadAsMap(compilationPackage);
        if (! classes.containsKey(name)) {
            throw new RuntimeException("Compile or load class failed!!");
        }
        return classes.get(name);
    }

    @Override
    public String toString() {
        composer.handleComposition(ClassBodyComposer.CodeCategory.CLASS, className);
        return composer.getCompleteClass();
    }

    public String getClassName() {
        return className;
    }

    /**
     * Compile Java Code.
     *
     * @return Class
     */
    @SuppressWarnings("unchecked")
    public Class<? extends Requirement> compile() {
        composer.handleComposition(ClassBodyComposer.CodeCategory.CLASS, className);
        String code = composer.getCompleteClass();
        QueryGenerator.close();
        LOGGER.debug("The Java Code is {}", code);
        /*
          CAUTION!!
          compile and load class in memory may throw NoClassFoundException,
          reason for that is current dynamic compiler has a bug which will be
          trigger when class loaded has multiple level inner class.
         */
        try {
            return compileSourceAndLoadClass(code, className, "");
        } catch (InMemoryCompiler.CompilerException | ClassNotFoundException ex) {
            throw new RuntimeException("Compile failed!!", ex);
        }
    }

    private static class WithClassPathInMemoryCompiler extends InMemoryCompiler {

        JavaCompiler getSystemJavaCompiler() {
            return ToolProvider.getSystemJavaCompiler();
        }

        DiagnosticCollector<JavaFileObject> getDiagnosticCollector() {
            return new DiagnosticCollector<>();
        }

        InMemoryClassManager getClassManager(JavaCompiler compiler) {
            return new InMemoryClassManager(compiler.getStandardFileManager(null, null, null));
        }

        public CompilationPackage singleCompile(String className, String code, String extraJars)
            throws InMemoryCompiler.CompilerException {
            return this.compile(ImmutableMap.<String, String>builder().put(className, code).build(), extraJars);
        }

        public CompilationPackage compile(Map<String, String> classesToCompile, String extraJars)
            throws CompilerException {
            //modified classpath acquirement mode
            String belongingJarPath = ClassBodyWrapper.class.getProtectionDomain().getCodeSource()
                .getLocation()
                .getPath();

            List<String> options = Arrays
                .asList("-classpath", extraJars
                    + System.getProperty("path.separator")
                    + System.getProperty("java.class.path")
                    + System.getProperty("path.separator")
                    + belongingJarPath
                );

            List<JavaSourceFromString> strFiles = Lists.newArrayList();
            Iterator it = classesToCompile.keySet().iterator();

            String compilationReport;
            while (it.hasNext()) {
                String className = (String) it.next();
                compilationReport = classesToCompile.get(className);
                strFiles.add(new JavaSourceFromString(className, compilationReport));
            }

            JavaCompiler compiler = this.getSystemJavaCompiler();
            DiagnosticCollector<JavaFileObject> collector = this.getDiagnosticCollector();
            InMemoryClassManager manager = this.getClassManager(compiler);
            JavaCompiler.CompilationTask task = compiler.getTask(null, manager,
                collector, options, null, strFiles);
            boolean status = task.call();
            if (status) {
                List<CompilationUnit> compilationUnits = manager.getAllClasses();
                return new CompilationPackage(compilationUnits);
            } else {
                compilationReport = this.buildCompilationReport(collector, options);
                throw new CompilerException(compilationReport);
            }
        }

        String buildCompilationReport(DiagnosticCollector<JavaFileObject> collector,
            List<String> options) {
            int count = 0;
            StringBuilder resultBuilder = new StringBuilder();

            for (Object o : collector.getDiagnostics()) {
                Diagnostic<?> diagnostic = (Diagnostic) o;
                ++ count;
                JavaSourceFromString javaSource = (JavaSourceFromString) diagnostic.getSource();
                resultBuilder.append(javaSource.getCharContent(false)).append("\n");
                resultBuilder.append("Compiler options: ").append(options).append("\n\n");
                resultBuilder.append(diagnostic.getKind()).append("|").append(diagnostic.getCode())
                    .append("\n");
                resultBuilder.append("LINE:COLUMN ").append(diagnostic.getLineNumber()).append(":")
                    .append(diagnostic.getColumnNumber()).append("\n")
                    .append(diagnostic.getMessage(null)).append("\n\n");
            }

            String diagnosticString = resultBuilder.toString();
            String compilationErrorsOverview = count + " class(es) failed to compile";
            return "Compilation error\n" + compilationErrorsOverview + "\n" + diagnosticString;
        }
    }
}
