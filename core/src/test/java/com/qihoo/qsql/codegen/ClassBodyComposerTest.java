package com.qihoo.qsql.codegen;

import com.qihoo.qsql.codegen.ClassBodyComposer.CodeCategory;
import com.qihoo.qsql.codegen.spark.SparkBodyWrapper;
import org.junit.Assert;
import org.junit.Test;

public class ClassBodyComposerTest {

    @Test
    public void testBasicCompositionFunction() {
        ClassBodyComposer composer = new ClassBodyComposer(SparkBodyWrapper.class);

        composer.handleComposition(CodeCategory.IMPORT,
            "import java.util.List");

        composer.handleComposition(CodeCategory.METHOD,
            "        public int func(String a, String b) {\n"
                + "            return (a + b).length();\n"
                + "        }");

        composer.handleComposition(CodeCategory.IMPORT, "import java.util.Map");
        composer.handleComposition(CodeCategory.CLASS, "TestRequirement");
        composer.handleComposition(CodeCategory.SENTENCE, "String str = \"Test\";");
        composer.handleComposition(CodeCategory.SENTENCE, "System.out.println(str);");

        Assert.assertEquals("import java.util.Map;\n"
                + "import java.util.List;\n"
                + "\n"
                + "public class TestRequirement extends SparkRequirement { \n"
                + "\t\tpublic TestRequirement(SparkSession spark){\n"
                + "\t\t\tsuper(spark);\n"
                + "\t\t}\n"
                + "        public int func(String a, String b) {\n"
                + "            return (a + b).length();\n"
                + "        }\n"
                + "\n"
                + "\t\tpublic void execute(){\n"
                + "\t\t\tDataset<Row> tmp;\n"
                + "\t\t\tString str = \"Test\";\n"
                + "\t\t\tSystem.out.println(str);\n"
                + "\t\t}\n"
                + "}\n",
            composer.getCompleteClass());
    }

    @Test
    public void testDuplicatedImport() {
        ClassBodyComposer composer = new ClassBodyComposer(SparkBodyWrapper.class);
        composer.handleComposition(CodeCategory.IMPORT,
            "import java.util.List");
        composer.handleComposition(CodeCategory.IMPORT,
            "import java.util.List");
        composer.handleComposition(CodeCategory.IMPORT,
            "import java.util.List");
        composer.handleComposition(CodeCategory.CLASS, "DefaultRequirement");

        Assert.assertEquals(
            "import java.util.List;\n"
                + "\n"
                + "public class DefaultRequirement extends SparkRequirement { \n"
                + "\t\tpublic DefaultRequirement(SparkSession spark){\n"
                + "\t\t\tsuper(spark);\n"
                + "\t\t}\n"
                + "\n"
                + "\t\tpublic void execute(){\n"
                + "\t\t\tDataset<Row> tmp;\n"
                + "\t\t}\n"
                + "}\n",
            composer.getCompleteClass());
    }

    @Test
    public void testComposeMultipleInnerClass() {
        ClassBodyComposer composer = new ClassBodyComposer(SparkBodyWrapper.class);
        composer.handleComposition(CodeCategory.INNER_CLASS,
            "      static class Animal { \n"
                + "             public String name;\n"
                + "         }");
        composer.handleComposition(CodeCategory.INNER_CLASS,
            "      static class Rabbit extends Animal { \n"
                + "             public String color;\n"
                + "         }");

        Assert.assertEquals("\n"
            + "public class DefaultRequirement_0 extends SparkRequirement { \n"
            + "\t\tpublic DefaultRequirement_0(SparkSession spark){\n"
            + "\t\t\tsuper(spark);\n"
            + "\t\t}\n"
            + "      static class Animal { \n"
            + "             public String name;\n"
            + "         }\n"
            + "      static class Rabbit extends Animal { \n"
            + "             public String color;\n"
            + "         }\n"
            + "\n"
            + "\t\tpublic void execute(){\n"
            + "\t\t\tDataset<Row> tmp;\n"
            + "\t\t}\n"
            + "}\n", composer.getCompleteClass());
    }

    @Test
    public void testNonClassNameClass() {
        ClassBodyComposer composer = new ClassBodyComposer(SparkBodyWrapper.class);
        try {
            composer.handleComposition(CodeCategory.CLASS);
            Assert.assertTrue(false);
        } catch (RuntimeException ex) {
            Assert.assertTrue(true);
        }
    }
}
