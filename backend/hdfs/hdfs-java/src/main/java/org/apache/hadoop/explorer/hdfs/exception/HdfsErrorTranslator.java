package org.apache.hadoop.explorer.hdfs.exception;

import org.springframework.http.HttpStatus;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HdfsErrorTranslator {

    private static final Pattern PERM_DENIED_PATTERN = Pattern.compile(
        "Permission denied: user=(?<user>[^,]+), access=(?<access>[^,]+), inode=\"(?<inode>[^\"]+)\"",
        Pattern.CASE_INSENSITIVE
    );

    public record ErrorResult(String userFriendlyMessage, HttpStatus status, String errorClass) {}

    public static ErrorResult translate(Throwable ex) {
        if (ex instanceof HdfsLocalizedException hle) {
            return new ErrorResult(hle.getMessage(), hle.getStatus(), hle.getErrorClass());
        }

        String className = ex.getClass().getSimpleName();
        String message = ex.getMessage() != null ? ex.getMessage() : "";
        String msgLower = message.toLowerCase();

        // 1. AccessControlException / Permission denied
        if (className.contains("AccessControlException") || msgLower.contains("permission denied")) {
            Matcher matcher = PERM_DENIED_PATTERN.matcher(message);
            if (matcher.find()) {
                String user = matcher.group("user");
                String access = matcher.group("access");
                String inode = matcher.group("inode");
                String accessRu = switch (access.toUpperCase()) {
                    case "READ" -> "чтение";
                    case "WRITE" -> "запись";
                    case "EXECUTE" -> "доступ/выполнение";
                    case "READ_EXECUTE" -> "чтение и доступ";
                    case "ALL" -> "полный доступ";
                    default -> access;
                };
                return new ErrorResult(
                    String.format("Отказано в доступе: у пользователя '%s' нет прав на %s для '%s'.", user, accessRu, inode),
                    HttpStatus.FORBIDDEN,
                    "AccessControlException"
                );
            }
            return new ErrorResult("Отказано в доступе (Permission denied): " + message, HttpStatus.FORBIDDEN, "AccessControlException");
        }

        // 2. FileNotFoundException
        if (className.contains("FileNotFoundException") || msgLower.contains("does not exist") || msgLower.contains("file not found")) {
            return new ErrorResult("Файл или директория не найдена: " + message, HttpStatus.NOT_FOUND, "FileNotFoundException");
        }

        // 3. FileAlreadyExistsException
        if (className.contains("FileAlreadyExistsException") || msgLower.contains("already exists")) {
            return new ErrorResult("Файл или директория уже существует: " + message, HttpStatus.CONFLICT, "FileAlreadyExistsException");
        }

        // 4. PathIsNotEmptyDirectoryException
        if (className.contains("PathIsNotEmptyDirectoryException") || msgLower.contains("is not empty")) {
            return new ErrorResult(
                "Каталог не пуст. Включите опцию рекурсивного удаления, чтобы удалить его вместе с содержимым.",
                HttpStatus.CONFLICT,
                "PathIsNotEmptyDirectoryException"
            );
        }

        // 5. SafeModeException
        if (className.contains("SafeModeException") || msgLower.contains("safe mode")) {
            return new ErrorResult(
                "Кластер HDFS находится в безопасном режиме (SafeMode). Запись временно заблокирована.",
                HttpStatus.SERVICE_UNAVAILABLE,
                "SafeModeException"
            );
        }

        // 6. StandbyException
        if (className.contains("StandbyException") || msgLower.contains("standby")) {
            return new ErrorResult(
                "NameNode находится в режиме ожидания (Standby). Запись невозможна.",
                HttpStatus.SERVICE_UNAVAILABLE,
                "StandbyException"
            );
        }

        // 7. QuotaExceededException
        if (className.contains("QuotaExceededException") || msgLower.contains("quota exceeded")) {
            return new ErrorResult("Превышена квота HDFS: " + message, HttpStatus.INSUFFICIENT_STORAGE, "QuotaExceededException");
        }

        // Fallback
        return new ErrorResult(message.isBlank() ? "Ошибка файловой системы HDFS" : message, HttpStatus.INTERNAL_SERVER_ERROR, className);
    }
}
