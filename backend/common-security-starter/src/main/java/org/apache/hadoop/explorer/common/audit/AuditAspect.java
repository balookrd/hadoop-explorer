package org.apache.hadoop.explorer.common.audit;

import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.CommonAuthenticationToken;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.time.Instant;
import java.util.Collections;

/**
 * AOP-аспект аудита методов, помеченных аннотацией @Audited.
 */
@Aspect
public class AuditAspect {

    private final AuditLogger auditLogger;
    private final String serviceName;

    public AuditAspect(AuditLogger auditLogger, String serviceName) {
        this.auditLogger = auditLogger;
        this.serviceName = serviceName != null ? serviceName : "hadoop-explorer-service";
    }

    @Around("@annotation(org.apache.hadoop.explorer.common.audit.Audited) || @within(org.apache.hadoop.explorer.common.audit.Audited)")
    public Object auditMethod(ProceedingJoinPoint joinPoint) throws Throwable {
        Audited audited = null;
        if (joinPoint.getSignature() instanceof org.aspectj.lang.reflect.MethodSignature methodSig) {
            audited = methodSig.getMethod().getAnnotation(Audited.class);
        }
        if (audited == null && joinPoint.getTarget() != null) {
            audited = joinPoint.getTarget().getClass().getAnnotation(Audited.class);
        }
        if (audited == null) {
            return joinPoint.proceed();
        }

        Instant start = Instant.now();
        String username = "anonymous";
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth instanceof CommonAuthenticationToken tokenAuth) {
            UserSession user = tokenAuth.getUserSession();
            if (user != null) {
                username = user.username();
            }
        }

        String action = audited.action().isEmpty() ? joinPoint.getSignature().getName() : audited.action();
        String resource = audited.resource().isEmpty() ? joinPoint.getTarget().getClass().getSimpleName() : audited.resource();

        try {
            Object result = joinPoint.proceed();
            long duration = Instant.now().toEpochMilli() - start.toEpochMilli();
            auditLogger.logEvent(new AuditRecord(
                start,
                serviceName,
                username,
                "internal",
                action,
                resource,
                "SUCCESS",
                duration,
                Collections.emptyMap()
            ));
            return result;
        } catch (Throwable t) {
            long duration = Instant.now().toEpochMilli() - start.toEpochMilli();
            auditLogger.logEvent(new AuditRecord(
                start,
                serviceName,
                username,
                "internal",
                action,
                resource,
                "FAILED: " + t.getMessage(),
                duration,
                Collections.emptyMap()
            ));
            throw t;
        }
    }
}
