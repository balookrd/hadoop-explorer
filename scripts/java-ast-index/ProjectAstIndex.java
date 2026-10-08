import com.sun.source.tree.*;
import com.sun.source.util.*;

import javax.lang.model.element.Modifier;
import javax.tools.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Автономный генератор AST-индекса Java для репозиториев (Java 17+).
 * Не требует внешних зависимостей, использует com.sun.source API из JDK.
 */
public class ProjectAstIndex {

    private static final Set<String> IGNORED_DIRS = Set.of(
            "target", "build", ".gradle", ".idea", ".git", ".venv",
            "node_modules", "dist", "data", "tmp", ".agents", ".context", "scripts"
    );

    private static final Set<String> COMMON_METHODS = Set.of(
            "equals", "hashCode", "toString", "getClass", "notify", "notifyAll", "wait"
    );

    public static class ClassDef {
        public String name;
        public String qualifiedName;
        public String kind; // CLASS, INTERFACE, ENUM, RECORD, ANNOTATION_TYPE
        public String path;
        public int line;
        public String extendsClause = "-";
        public List<String> implementsList = new ArrayList<>();
        public List<String> annotations = new ArrayList<>();
        public List<FieldDef> fields = new ArrayList<>();
        public List<MethodDef> methods = new ArrayList<>();
        public boolean isTest;
        public List<String> imports = new ArrayList<>();
    }

    public static class FieldDef {
        public String name;
        public String type;
        public String ownerClass;
        public int line;
        public String path;
        public List<String> annotations = new ArrayList<>();
        public boolean isStatic;
        public boolean isFinal;
    }

    public static class MethodDef {
        public String name;
        public String ownerClass;
        public String returnType;
        public List<String> parameters = new ArrayList<>();
        public List<String> paramTypes = new ArrayList<>();
        public List<String> annotations = new ArrayList<>();
        public int line;
        public String path;
        public boolean isConstructor;
        public Set<String> calls = new LinkedHashSet<>();
        public Set<String> news = new LinkedHashSet<>();
    }

    public static class SymbolRecord {
        public String kind;
        public String name;
        public String qualifiedName;
        public String owner;
        public String path;
        public int line;
        public String detail;

        public SymbolRecord(String kind, String name, String qualifiedName, String owner, String path, int line, String detail) {
            this.kind = kind;
            this.name = name;
            this.qualifiedName = qualifiedName;
            this.owner = owner;
            this.path = path;
            this.line = line;
            this.detail = detail;
        }
    }

    public static class AffectedTestRecord {
        public String prodClass;
        public String testClass;
        public String testPath;
        public Set<String> reasons = new LinkedHashSet<>();

        public AffectedTestRecord(String prodClass, String testClass, String testPath) {
            this.prodClass = prodClass;
            this.testClass = testClass;
            this.testPath = testPath;
        }
    }

    public static void main(String[] args) {
        Path repoRoot = Paths.get(args.length > 0 ? args[0] : ".").toAbsolutePath().normalize();
        Path outputTarget = Paths.get(args.length > 1 ? args[1] : "target/java-ast-index/java-symbols.json");
        if (!outputTarget.isAbsolute()) {
            outputTarget = repoRoot.resolve(outputTarget).normalize();
        }

        Path outputDir = outputTarget.getParent() != null ? outputTarget.getParent() : repoRoot.resolve("target/java-ast-index");
        Path jsonFile = outputTarget.getFileName().toString().endsWith(".json")
                ? outputTarget
                : outputDir.resolve("java-symbols.json");

        try {
            Files.createDirectories(outputDir);
        } catch (IOException e) {
            System.err.println("Ошибка создания каталога вывода: " + outputDir + " (" + e.getMessage() + ")");
            System.exit(1);
        }

        System.out.println("[ProjectAstIndex] Сканирование Java исходников в: " + repoRoot);
        List<Path> javaFiles = findJavaFiles(repoRoot);
        System.out.println("[ProjectAstIndex] Найдено Java-файлов: " + javaFiles.size());

        if (javaFiles.isEmpty()) {
            System.out.println("[ProjectAstIndex] Внимание: Java файлы в src/main/java и src/test/java не найдены.");
            writeEmptyIndex(outputDir, jsonFile, repoRoot);
            return;
        }

        List<ClassDef> classes = new ArrayList<>();
        List<SymbolRecord> symbols = new ArrayList<>();
        Map<String, List<String>> callReferences = new HashMap<>();
        Map<String, List<String>> newReferences = new HashMap<>();

        parseAndIndex(repoRoot, javaFiles, classes, symbols, callReferences, newReferences);

        // Расчет affected tests
        List<AffectedTestRecord> affectedTests = computeAffectedTests(classes);

        // Запись результатов
        System.out.println("[ProjectAstIndex] Запись TSV и JSON в: " + outputDir);
        writeSymbolsTsv(outputDir.resolve("symbols.tsv"), symbols);
        writeClassesTsv(outputDir.resolve("classes.tsv"), classes);
        writeMethodsTsv(outputDir.resolve("methods.tsv"), classes);
        writeAggregatedTsv(outputDir.resolve("calls.tsv"), "call", callReferences);
        writeAggregatedTsv(outputDir.resolve("news.tsv"), "new", newReferences);
        writeAffectedTestsTsv(outputDir.resolve("affected-tests.tsv"), affectedTests);
        writeJson(jsonFile, repoRoot, classes, affectedTests, callReferences, newReferences, javaFiles.size(), symbols.size());

        System.out.println("[ProjectAstIndex] Индексация успешно завершена:");
        System.out.println("  - Классов/типов: " + classes.size());
        System.out.println("  - Символов:      " + symbols.size());
        System.out.println("  - Вызовов (calls):" + callReferences.size());
        System.out.println("  - Созданий (new): " + newReferences.size());
        System.out.println("  - Связей тестов: " + affectedTests.size());
        System.out.println("  - Файлы индекса: " + outputDir);
    }

    private static List<Path> findJavaFiles(Path repoRoot) {
        List<Path> result = new ArrayList<>();
        try {
            Files.walkFileTree(repoRoot, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    String name = dir.getFileName() != null ? dir.getFileName().toString() : "";
                    if (IGNORED_DIRS.contains(name)) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    String name = file.getFileName().toString();
                    if (name.endsWith(".java") && !name.equals("ProjectAstIndex.java")) {
                        String normalized = repoRoot.relativize(file).toString().replace('\\', '/');
                        if (normalized.contains("/src/main/java/") || normalized.startsWith("src/main/java/")
                                || normalized.contains("/src/test/java/") || normalized.startsWith("src/test/java/")) {
                            result.add(file);
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            System.err.println("Ошибка обхода файлов: " + e.getMessage());
        }
        result.sort(Comparator.comparing(Path::toString));
        return result;
    }

    private static void parseAndIndex(
            Path repoRoot,
            List<Path> javaFiles,
            List<ClassDef> classes,
            List<SymbolRecord> symbols,
            Map<String, List<String>> callReferences,
            Map<String, List<String>> newReferences
    ) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new IllegalStateException("System Java Compiler не найден. Убедитесь, что запуск выполняется под JDK, а не JRE.");
        }

        StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, StandardCharsets.UTF_8);
        try {
            // Парсим пачками для оптимизации памяти
            int batchSize = 100;
            for (int i = 0; i < javaFiles.size(); i += batchSize) {
                int end = Math.min(i + batchSize, javaFiles.size());
                List<Path> batch = javaFiles.subList(i, end);

                Iterable<? extends JavaFileObject> fileObjects = fileManager.getJavaFileObjectsFromPaths(batch);
                JavacTask task = (JavacTask) compiler.getTask(null, fileManager, null, List.of("-proc:none"), null, fileObjects);
                Trees trees = Trees.instance(task);
                SourcePositions sourcePositions = trees.getSourcePositions();

                Iterable<? extends CompilationUnitTree> asts = task.parse();
                for (CompilationUnitTree cu : asts) {
                    processCompilationUnit(repoRoot, cu, sourcePositions, classes, symbols, callReferences, newReferences);
                }
            }
        } catch (Exception e) {
            System.err.println("Ошибка при парсинге AST: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                fileManager.close();
            } catch (IOException ignored) {}
        }
    }

    private static void processCompilationUnit(
            Path repoRoot,
            CompilationUnitTree cu,
            SourcePositions sourcePositions,
            List<ClassDef> classes,
            List<SymbolRecord> symbols,
            Map<String, List<String>> callReferences,
            Map<String, List<String>> newReferences
    ) {
        if (cu.getSourceFile() == null) return;
        Path filePath = Paths.get(cu.getSourceFile().toUri()).toAbsolutePath().normalize();
        String relPath = repoRoot.relativize(filePath).toString().replace('\\', '/');

        boolean isTestPath = relPath.contains("/src/test/java/") || relPath.startsWith("src/test/java/") || relPath.endsWith("Test.java");
        String packageName = cu.getPackageName() != null ? cu.getPackageName().toString() : "";
        LineMap lineMap = cu.getLineMap();

        List<String> imports = new ArrayList<>();
        for (ImportTree imp : cu.getImports()) {
            imports.add(imp.getQualifiedIdentifier().toString());
        }

        Deque<ClassDef> classStack = new ArrayDeque<>();

        TreeScanner<Void, Void> scanner = new TreeScanner<>() {
            @Override
            public Void visitClass(ClassTree classTree, Void unused) {
                String simpleName = classTree.getSimpleName().toString();
                if (simpleName.isEmpty()) {
                    simpleName = "$Anon_" + getNodeLine(cu, classTree, sourcePositions, lineMap);
                }

                String qualifiedName;
                if (classStack.isEmpty()) {
                    qualifiedName = packageName.isEmpty() ? simpleName : packageName + "." + simpleName;
                } else {
                    qualifiedName = classStack.peek().qualifiedName + "." + simpleName;
                }

                int classLine = getNodeLine(cu, classTree, sourcePositions, lineMap);

                ClassDef classDef = new ClassDef();
                classDef.name = simpleName;
                classDef.qualifiedName = qualifiedName;
                classDef.kind = classTree.getKind().name();
                classDef.path = relPath;
                classDef.line = classLine;
                classDef.isTest = isTestPath || simpleName.endsWith("Test") || simpleName.endsWith("Tests") || simpleName.endsWith("IT");
                classDef.imports.addAll(imports);

                if (classTree.getExtendsClause() != null) {
                    classDef.extendsClause = classTree.getExtendsClause().toString();
                }
                if (classTree.getImplementsClause() != null && !classTree.getImplementsClause().isEmpty()) {
                    for (Tree impl : classTree.getImplementsClause()) {
                        classDef.implementsList.add(impl.toString());
                    }
                }
                if (classTree.getModifiers() != null && classTree.getModifiers().getAnnotations() != null) {
                    for (AnnotationTree ann : classTree.getModifiers().getAnnotations()) {
                        classDef.annotations.add(ann.getAnnotationType().toString());
                    }
                }

                classes.add(classDef);
                classStack.push(classDef);

                // Регистрируем символ класса
                String classDetail = "extends: " + classDef.extendsClause;
                if (!classDef.implementsList.isEmpty()) {
                    classDetail += ", implements: " + String.join(",", classDef.implementsList);
                }
                symbols.add(new SymbolRecord(classDef.kind, classDef.name, classDef.qualifiedName,
                        packageName.isEmpty() ? "-" : packageName, relPath, classLine, classDetail));

                // Обходим члены класса (поля, методы, вложенные классы)
                for (Tree member : classTree.getMembers()) {
                    if (member instanceof VariableTree vt) {
                        processField(vt, classDef, relPath, cu, sourcePositions, lineMap, symbols);
                    } else if (member instanceof MethodTree mt) {
                        processMethod(mt, classDef, relPath, cu, sourcePositions, lineMap, symbols, callReferences, newReferences);
                    } else if (member instanceof ClassTree innerCt) {
                        innerCt.accept(this, null);
                    }
                }

                classStack.pop();
                return null;
            }
        };

        for (Tree typeDecl : cu.getTypeDecls()) {
            typeDecl.accept(scanner, null);
        }
    }

    private static void processField(
            VariableTree vt,
            ClassDef classDef,
            String relPath,
            CompilationUnitTree cu,
            SourcePositions sourcePositions,
            LineMap lineMap,
            List<SymbolRecord> symbols
    ) {
        String fieldName = vt.getName().toString();
        String fieldType = vt.getType() != null ? vt.getType().toString() : "var";
        int line = getNodeLine(cu, vt, sourcePositions, lineMap);

        boolean isStatic = vt.getModifiers() != null && vt.getModifiers().getFlags().contains(Modifier.STATIC);
        boolean isFinal = vt.getModifiers() != null && vt.getModifiers().getFlags().contains(Modifier.FINAL);
        boolean isConstant = isStatic && isFinal;

        FieldDef fieldDef = new FieldDef();
        fieldDef.name = fieldName;
        fieldDef.type = fieldType;
        fieldDef.ownerClass = classDef.qualifiedName;
        fieldDef.line = line;
        fieldDef.path = relPath;
        fieldDef.isStatic = isStatic;
        fieldDef.isFinal = isFinal;

        if (vt.getModifiers() != null && vt.getModifiers().getAnnotations() != null) {
            for (AnnotationTree ann : vt.getModifiers().getAnnotations()) {
                fieldDef.annotations.add(ann.getAnnotationType().toString());
            }
        }
        classDef.fields.add(fieldDef);

        String symbolKind = isConstant ? "CONSTANT" : "FIELD";
        String qualifiedField = classDef.qualifiedName + "#" + fieldName;
        symbols.add(new SymbolRecord(symbolKind, fieldName, qualifiedField, classDef.qualifiedName, relPath, line, fieldType));
    }

    private static void processMethod(
            MethodTree mt,
            ClassDef classDef,
            String relPath,
            CompilationUnitTree cu,
            SourcePositions sourcePositions,
            LineMap lineMap,
            List<SymbolRecord> symbols,
            Map<String, List<String>> callReferences,
            Map<String, List<String>> newReferences
    ) {
        String methodName = mt.getName().toString();
        boolean isConstructor = "<init>".equals(methodName) || methodName.equals(classDef.name);
        String displayName = isConstructor ? "<init>" : methodName;
        String returnType = isConstructor ? "<init>" : (mt.getReturnType() != null ? mt.getReturnType().toString() : "void");
        int line = getNodeLine(cu, mt, sourcePositions, lineMap);

        MethodDef methodDef = new MethodDef();
        methodDef.name = displayName;
        methodDef.ownerClass = classDef.qualifiedName;
        methodDef.returnType = returnType;
        methodDef.line = line;
        methodDef.path = relPath;
        methodDef.isConstructor = isConstructor;

        for (VariableTree pt : mt.getParameters()) {
            String pType = pt.getType() != null ? pt.getType().toString() : "Object";
            methodDef.parameters.add(pType + " " + pt.getName());
            methodDef.paramTypes.add(pType);
        }

        if (mt.getModifiers() != null && mt.getModifiers().getAnnotations() != null) {
            for (AnnotationTree ann : mt.getModifiers().getAnnotations()) {
                methodDef.annotations.add(ann.getAnnotationType().toString());
            }
        }

        // Анализ тела метода на calls и news
        if (mt.getBody() != null) {
            mt.getBody().accept(new TreeScanner<Void, Void>() {
                @Override
                public Void visitMethodInvocation(MethodInvocationTree invocation, Void unused) {
                    ExpressionTree select = invocation.getMethodSelect();
                    String callName = extractMethodCallName(select);
                    if (!callName.isEmpty()) {
                        methodDef.calls.add(callName);
                        int callLine = getNodeLine(cu, invocation, sourcePositions, lineMap);
                        String ref = relPath + ":" + callLine + "(" + classDef.name + "#" + displayName + ")";
                        callReferences.computeIfAbsent(callName, k -> new ArrayList<>()).add(ref);
                    }
                    return super.visitMethodInvocation(invocation, unused);
                }

                @Override
                public Void visitNewClass(NewClassTree newClass, Void unused) {
                    ExpressionTree id = newClass.getIdentifier();
                    if (id != null) {
                        String fullTypeName = id.toString();
                        String simpleTypeName = extractSimpleTypeName(fullTypeName);
                        if (!simpleTypeName.isEmpty()) {
                            methodDef.news.add(simpleTypeName);
                            int newLine = getNodeLine(cu, newClass, sourcePositions, lineMap);
                            String ref = relPath + ":" + newLine + "(" + classDef.name + "#" + displayName + ")";
                            newReferences.computeIfAbsent(simpleTypeName, k -> new ArrayList<>()).add(ref);
                        }
                    }
                    return super.visitNewClass(newClass, unused);
                }
            }, null);
        }

        classDef.methods.add(methodDef);

        String symbolKind = isConstructor ? "CONSTRUCTOR" : "METHOD";
        String qualifiedMethod = classDef.qualifiedName + "#" + displayName + "(" + String.join(",", methodDef.paramTypes) + ")";
        String signatureDetail = returnType + "(" + String.join(", ", methodDef.parameters) + ")";
        symbols.add(new SymbolRecord(symbolKind, displayName, qualifiedMethod, classDef.qualifiedName, relPath, line, signatureDetail));
    }

    private static String extractMethodCallName(ExpressionTree select) {
        if (select == null) return "";
        if (select instanceof MemberSelectTree mst) {
            return mst.getIdentifier().toString();
        } else if (select instanceof IdentifierTree id) {
            return id.getName().toString();
        } else {
            String str = select.toString();
            int dot = str.lastIndexOf('.');
            return dot >= 0 ? str.substring(dot + 1) : str;
        }
    }

    private static String extractSimpleTypeName(String typeStr) {
        if (typeStr == null) return "";
        String clean = typeStr.replaceAll("<.*>", "").trim();
        int dot = clean.lastIndexOf('.');
        return dot >= 0 ? clean.substring(dot + 1) : clean;
    }

    private static int getNodeLine(CompilationUnitTree cu, Tree node, SourcePositions sp, LineMap lm) {
        if (node == null || sp == null || lm == null) return 1;
        long pos = sp.getStartPosition(cu, node);
        if (pos >= 0) {
            long line = lm.getLineNumber(pos);
            return (int) line;
        }
        return 1;
    }

    private static List<AffectedTestRecord> computeAffectedTests(List<ClassDef> classes) {
        List<ClassDef> testClasses = classes.stream().filter(c -> c.isTest).toList();
        List<ClassDef> prodClasses = classes.stream().filter(c -> !c.isTest).toList();

        Map<String, AffectedTestRecord> matches = new LinkedHashMap<>();

        for (ClassDef test : testClasses) {
            String testSimple = test.name;
            String baseName = deriveBaseNameFromTest(testSimple);

            Set<String> testNewTypes = new HashSet<>();
            Set<String> testCalls = new HashSet<>();
            Set<String> testFieldTypes = new HashSet<>();

            for (FieldDef f : test.fields) {
                testFieldTypes.add(extractSimpleTypeName(f.type));
            }
            for (MethodDef m : test.methods) {
                testNewTypes.addAll(m.news);
                testCalls.addAll(m.calls);
            }

            for (ClassDef prod : prodClasses) {
                Set<String> reasons = new LinkedHashSet<>();

                // 1. По имени (FooTest -> Foo)
                if (!baseName.isEmpty() && (baseName.equals(prod.name) || prod.name.startsWith(baseName))) {
                    reasons.add("NAME_MATCH");
                }

                // 2. По импортам
                if (test.imports.contains(prod.qualifiedName)) {
                    reasons.add("IMPORT");
                }

                // 3. По new
                if (testNewTypes.contains(prod.name)) {
                    reasons.add("NEW_INSTANCE");
                }

                // 4. По типу полей (например @Mock Foo foo)
                if (testFieldTypes.contains(prod.name)) {
                    reasons.add("FIELD_TYPE");
                }

                // 5. По уникальным вызовам методов
                if (!prod.methods.isEmpty()) {
                    for (MethodDef pm : prod.methods) {
                        if (!pm.isConstructor && !COMMON_METHODS.contains(pm.name) && pm.name.length() > 3) {
                            if (testCalls.contains(pm.name)) {
                                reasons.add("METHOD_CALL");
                                break;
                            }
                        }
                    }
                }

                if (!reasons.isEmpty()) {
                    String key = prod.qualifiedName + "->" + test.qualifiedName;
                    AffectedTestRecord record = matches.computeIfAbsent(key, k -> new AffectedTestRecord(prod.qualifiedName, test.qualifiedName, test.path));
                    record.reasons.addAll(reasons);
                }
            }
        }

        List<AffectedTestRecord> result = new ArrayList<>(matches.values());
        result.sort(Comparator.comparing((AffectedTestRecord r) -> r.prodClass).thenComparing(r -> r.testClass));
        return result;
    }

    private static String deriveBaseNameFromTest(String testName) {
        String s = testName;
        if (s.startsWith("Test")) {
            s = s.substring(4);
        }
        if (s.endsWith("IntegrationTest")) {
            s = s.substring(0, s.length() - "IntegrationTest".length());
        } else if (s.endsWith("TestCase")) {
            s = s.substring(0, s.length() - "TestCase".length());
        } else if (s.endsWith("Tests")) {
            s = s.substring(0, s.length() - "Tests".length());
        } else if (s.endsWith("Test")) {
            s = s.substring(0, s.length() - "Test".length());
        } else if (s.endsWith("ITCase")) {
            s = s.substring(0, s.length() - "ITCase".length());
        } else if (s.endsWith("IT")) {
            s = s.substring(0, s.length() - "IT".length());
        }
        return s;
    }

    private static String sanitizeTsv(String val) {
        if (val == null || val.isBlank()) return "-";
        return val.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ').trim();
    }

    private static void writeSymbolsTsv(Path path, List<SymbolRecord> symbols) {
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writer.write("kind\tname\tqualifiedName\towner\tpath\tline\tdetail\n");
            for (SymbolRecord s : symbols) {
                writer.write(sanitizeTsv(s.kind) + "\t"
                        + sanitizeTsv(s.name) + "\t"
                        + sanitizeTsv(s.qualifiedName) + "\t"
                        + sanitizeTsv(s.owner) + "\t"
                        + sanitizeTsv(s.path) + "\t"
                        + s.line + "\t"
                        + sanitizeTsv(s.detail) + "\n");
            }
        } catch (IOException e) {
            System.err.println("Ошибка записи symbols.tsv: " + e.getMessage());
        }
    }

    private static void writeClassesTsv(Path path, List<ClassDef> classes) {
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writer.write("path\tline\tkind\tqualifiedName\textends\timplements\tannotations\tfields\n");
            for (ClassDef c : classes) {
                String impls = c.implementsList.isEmpty() ? "-" : String.join(",", c.implementsList);
                String anns = c.annotations.isEmpty() ? "-" : String.join(",", c.annotations);
                String fieldsStr = c.fields.isEmpty() ? "-" : c.fields.stream()
                        .map(f -> f.name + ":" + f.type)
                        .collect(Collectors.joining(","));

                writer.write(sanitizeTsv(c.path) + "\t"
                        + c.line + "\t"
                        + sanitizeTsv(c.kind) + "\t"
                        + sanitizeTsv(c.qualifiedName) + "\t"
                        + sanitizeTsv(c.extendsClause) + "\t"
                        + sanitizeTsv(impls) + "\t"
                        + sanitizeTsv(anns) + "\t"
                        + sanitizeTsv(fieldsStr) + "\n");
            }
        } catch (IOException e) {
            System.err.println("Ошибка записи classes.tsv: " + e.getMessage());
        }
    }

    private static void writeMethodsTsv(Path path, List<ClassDef> classes) {
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writer.write("path\tline\tclass\tmethod\treturnType\tparameters\tannotations\tcalls\tnews\n");
            for (ClassDef c : classes) {
                for (MethodDef m : c.methods) {
                    String params = m.parameters.isEmpty() ? "-" : String.join(",", m.parameters);
                    String anns = m.annotations.isEmpty() ? "-" : String.join(",", m.annotations);
                    String calls = m.calls.isEmpty() ? "-" : String.join(",", m.calls);
                    String news = m.news.isEmpty() ? "-" : String.join(",", m.news);

                    writer.write(sanitizeTsv(m.path) + "\t"
                            + m.line + "\t"
                            + sanitizeTsv(c.qualifiedName) + "\t"
                            + sanitizeTsv(m.name) + "\t"
                            + sanitizeTsv(m.returnType) + "\t"
                            + sanitizeTsv(params) + "\t"
                            + sanitizeTsv(anns) + "\t"
                            + sanitizeTsv(calls) + "\t"
                            + sanitizeTsv(news) + "\n");
                }
            }
        } catch (IOException e) {
            System.err.println("Ошибка записи methods.tsv: " + e.getMessage());
        }
    }

    private static void writeAggregatedTsv(Path path, String itemHeader, Map<String, List<String>> references) {
        List<Map.Entry<String, List<String>>> entries = new ArrayList<>(references.entrySet());
        entries.sort((a, b) -> Integer.compare(b.getValue().size(), a.getValue().size()));

        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writer.write(itemHeader + "\tcount\tsampleReferences\n");
            for (Map.Entry<String, List<String>> entry : entries) {
                String item = entry.getKey();
                int count = entry.getValue().size();
                String samples = entry.getValue().stream().limit(5).collect(Collectors.joining("; "));
                writer.write(sanitizeTsv(item) + "\t" + count + "\t" + sanitizeTsv(samples) + "\n");
            }
        } catch (IOException e) {
            System.err.println("Ошибка записи " + path.getFileName() + ": " + e.getMessage());
        }
    }

    private static void writeAffectedTestsTsv(Path path, List<AffectedTestRecord> records) {
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writer.write("prodClass\ttestClass\ttestPath\treasons\n");
            for (AffectedTestRecord r : records) {
                String reasonsStr = String.join(",", r.reasons);
                writer.write(sanitizeTsv(r.prodClass) + "\t"
                        + sanitizeTsv(r.testClass) + "\t"
                        + sanitizeTsv(r.testPath) + "\t"
                        + sanitizeTsv(reasonsStr) + "\n");
            }
        } catch (IOException e) {
            System.err.println("Ошибка записи affected-tests.tsv: " + e.getMessage());
        }
    }

    private static void writeEmptyIndex(Path outputDir, Path jsonFile, Path repoRoot) {
        try {
            Files.writeString(outputDir.resolve("symbols.tsv"), "kind\tname\tqualifiedName\towner\tpath\tline\tdetail\n", StandardCharsets.UTF_8);
            Files.writeString(outputDir.resolve("classes.tsv"), "path\tline\tkind\tqualifiedName\textends\timplements\tannotations\tfields\n", StandardCharsets.UTF_8);
            Files.writeString(outputDir.resolve("methods.tsv"), "path\tline\tclass\tmethod\treturnType\tparameters\tannotations\tcalls\tnews\n", StandardCharsets.UTF_8);
            Files.writeString(outputDir.resolve("calls.tsv"), "call\tcount\tsampleReferences\n", StandardCharsets.UTF_8);
            Files.writeString(outputDir.resolve("news.tsv"), "new\tcount\tsampleReferences\n", StandardCharsets.UTF_8);
            Files.writeString(outputDir.resolve("affected-tests.tsv"), "prodClass\ttestClass\ttestPath\treasons\n", StandardCharsets.UTF_8);
            Files.writeString(jsonFile, "{\"version\":1,\"generatedAt\":\"" + Instant.now() + "\",\"classes\":[],\"stats\":{\"files\":0}}\n", StandardCharsets.UTF_8);
        } catch (IOException ignored) {}
    }

    private static void writeJson(
            Path jsonFile,
            Path repoRoot,
            List<ClassDef> classes,
            List<AffectedTestRecord> affectedTests,
            Map<String, List<String>> callReferences,
            Map<String, List<String>> newReferences,
            int fileCount,
            int symbolCount
    ) {
        long methodCount = classes.stream().mapToLong(c -> c.methods.size()).sum();
        long fieldCount = classes.stream().mapToLong(c -> c.fields.size()).sum();

        StringBuilder sb = new StringBuilder(1024 * 64);
        sb.append("{\n");
        sb.append("  \"version\": 1,\n");
        sb.append("  \"generatedAt\": \"").append(Instant.now()).append("\",\n");
        sb.append("  \"repoRoot\": ").append(jsonEscape(repoRoot.toString())).append(",\n");
        sb.append("  \"stats\": {\n");
        sb.append("    \"files\": ").append(fileCount).append(",\n");
        sb.append("    \"classes\": ").append(classes.size()).append(",\n");
        sb.append("    \"methods\": ").append(methodCount).append(",\n");
        sb.append("    \"fields\": ").append(fieldCount).append(",\n");
        sb.append("    \"symbols\": ").append(symbolCount).append(",\n");
        sb.append("    \"calls\": ").append(callReferences.size()).append(",\n");
        sb.append("    \"news\": ").append(newReferences.size()).append(",\n");
        sb.append("    \"affectedTests\": ").append(affectedTests.size()).append("\n");
        sb.append("  },\n");

        sb.append("  \"classes\": [\n");
        for (int i = 0; i < classes.size(); i++) {
            ClassDef c = classes.get(i);
            sb.append("    {\n");
            sb.append("      \"name\": ").append(jsonEscape(c.name)).append(",\n");
            sb.append("      \"qualifiedName\": ").append(jsonEscape(c.qualifiedName)).append(",\n");
            sb.append("      \"kind\": ").append(jsonEscape(c.kind)).append(",\n");
            sb.append("      \"path\": ").append(jsonEscape(c.path)).append(",\n");
            sb.append("      \"line\": ").append(c.line).append(",\n");
            sb.append("      \"isTest\": ").append(c.isTest).append(",\n");
            sb.append("      \"extends\": ").append(jsonEscape(c.extendsClause)).append(",\n");
            sb.append("      \"implements\": [").append(c.implementsList.stream().map(ProjectAstIndex::jsonEscape).collect(Collectors.joining(","))).append("],\n");
            sb.append("      \"annotations\": [").append(c.annotations.stream().map(ProjectAstIndex::jsonEscape).collect(Collectors.joining(","))).append("],\n");
            sb.append("      \"fields\": [\n");
            for (int fi = 0; fi < c.fields.size(); fi++) {
                FieldDef f = c.fields.get(fi);
                sb.append("        {\"name\": ").append(jsonEscape(f.name))
                        .append(", \"type\": ").append(jsonEscape(f.type))
                        .append(", \"line\": ").append(f.line).append("}");
                if (fi < c.fields.size() - 1) sb.append(",");
                sb.append("\n");
            }
            sb.append("      ],\n");
            sb.append("      \"methods\": [\n");
            for (int mi = 0; mi < c.methods.size(); mi++) {
                MethodDef m = c.methods.get(mi);
                sb.append("        {\"name\": ").append(jsonEscape(m.name))
                        .append(", \"returnType\": ").append(jsonEscape(m.returnType))
                        .append(", \"parameters\": ").append(jsonEscape(String.join(", ", m.parameters)))
                        .append(", \"line\": ").append(m.line)
                        .append(", \"calls\": [").append(m.calls.stream().map(ProjectAstIndex::jsonEscape).collect(Collectors.joining(","))).append("]")
                        .append(", \"news\": [").append(m.news.stream().map(ProjectAstIndex::jsonEscape).collect(Collectors.joining(","))).append("]}");
                if (mi < c.methods.size() - 1) sb.append(",");
                sb.append("\n");
            }
            sb.append("      ]\n");
            sb.append("    }");
            if (i < classes.size() - 1) sb.append(",");
            sb.append("\n");
        }
        sb.append("  ],\n");

        sb.append("  \"affectedTests\": [\n");
        for (int i = 0; i < affectedTests.size(); i++) {
            AffectedTestRecord at = affectedTests.get(i);
            sb.append("    {\"prodClass\": ").append(jsonEscape(at.prodClass))
                    .append(", \"testClass\": ").append(jsonEscape(at.testClass))
                    .append(", \"testPath\": ").append(jsonEscape(at.testPath))
                    .append(", \"reasons\": [").append(at.reasons.stream().map(ProjectAstIndex::jsonEscape).collect(Collectors.joining(","))).append("]}");
            if (i < affectedTests.size() - 1) sb.append(",");
            sb.append("\n");
        }
        sb.append("  ]\n");
        sb.append("}\n");

        try {
            Files.writeString(jsonFile, sb.toString(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("Ошибка записи json: " + e.getMessage());
        }
    }

    private static String jsonEscape(String s) {
        if (s == null) return "\"\"";
        StringBuilder sb = new StringBuilder(s.length() + 8);
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < ' ') {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
        return sb.toString();
    }
}
