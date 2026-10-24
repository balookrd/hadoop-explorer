package org.apache.hadoop.explorer.replicator.security;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Валидация Kerberos-принципалов имперсонации: запрет doAs под системными учетками ОС
 * и защита от инъекций в имени принципала.
 *
 * <p>Проверка живет отдельно от {@code KerberosUgiProvider}, потому что нужна и оркестратору,
 * который принимает принципал от пользователя в REST-запросе, и агенту, который выполняет под
 * ним doAs. Сам провайдер UGI тянет Hadoop-стек, и ради одной статической проверки держать
 * его в classpath control plane незачем.
 */
public final class PrincipalSafety {

    /** Системные учетные записи ОС: имперсонация под ними запрещена. */
    public static final Set<String> FORBIDDEN_PRINCIPALS = Set.of(
            "root", "daemon", "bin", "sys", "sync", "games", "man", "lp", "mail", "news",
            "uucp", "proxy", "adm", "shutdown", "halt", "operator", "nobody", "0"
    );

    public static final Pattern SAFE_PRINCIPAL_PATTERN =
            Pattern.compile("^[a-zA-Z0-9_.-]+(/[a-zA-Z0-9_.-]+)?(@[a-zA-Z0-9_.-]+)?$");

    private PrincipalSafety() {
    }

    /**
     * Извлечение чистого имени пользователя из Kerberos Principal (например 'alice@REALM.LOCAL' -> 'alice').
     */
    public static String extractUsername(String principal) {
        if (principal == null || principal.isBlank()) {
            return "hdfs";
        }
        String p = principal.trim();
        int atIdx = p.indexOf('@');
        if (atIdx > 0) {
            p = p.substring(0, atIdx);
        }
        int slashIdx = p.indexOf('/');
        if (slashIdx > 0) {
            p = p.substring(0, slashIdx);
        }
        return p;
    }

    /**
     * Валидация безопасности принципала имперсонации.
     *
     * @throws SecurityException если принципал содержит инъекцию или указывает на системную учетку
     */
    public static void validate(String principal) {
        if (principal == null || principal.isBlank()) {
            return;
        }
        String trimmed = principal.trim();
        if (trimmed.contains("..") || trimmed.contains("\0") || !SAFE_PRINCIPAL_PATTERN.matcher(trimmed).matches()) {
            throw new SecurityException("Попытка инъекции или недопустимые символы в имени принципала: " + principal);
        }
        String username = extractUsername(trimmed).toLowerCase(Locale.ROOT);
        if (FORBIDDEN_PRINCIPALS.contains(username)) {
            throw new SecurityException("Попытка имперсонации запрещенной системной учетной записи: " + principal);
        }
    }
}
