package org.apache.hadoop.explorer.common.security;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.security.web.util.matcher.IpAddressMatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves the client address used as a rate-limit key.
 *
 * <p>{@code X-Forwarded-For} is honoured only when the direct peer is a trusted proxy. All header
 * lines are joined in order into one chain, which is walked right to left; the first hop that is not a trusted proxy is the client. A hop that is
 * not an IP literal stops the walk. Any other peer is keyed by its socket address, so a client
 * cannot choose its own key by forging the header.
 */
public class ClientIpResolver {

    static final String FORWARDED_FOR = "X-Forwarded-For";

    private static final Pattern IPV4 = Pattern.compile("^(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})$");
    private static final Pattern IPV6 = Pattern.compile("^[0-9a-fA-F:.]+$");

    private final List<IpAddressMatcher> trustedProxies;

    public ClientIpResolver(List<String> trustedProxies) {
        this.trustedProxies = trustedProxies == null ? List.of() : trustedProxies.stream()
            .map(String::trim)
            .filter(entry -> !entry.isEmpty())
            .map(IpAddressMatcher::new)
            .toList();
    }

    public String resolve(HttpServletRequest request) {
        String peer = request.getRemoteAddr();
        if (!isTrusted(peer)) {
            return peer;
        }
        String[] hops = forwardedChain(request);
        if (hops.length == 0) {
            return peer;
        }
        String candidate = peer;
        for (int i = hops.length - 1; i >= 0; i--) {
            String hop = hops[i].trim();
            if (!isIpLiteral(hop)) {
                break;
            }
            candidate = hop;
            if (!isTrusted(hop)) {
                break;
            }
        }
        return candidate;
    }

    /**
     * Joins every {@code X-Forwarded-For} header line in arrival order. A proxy such as HAProxy with
     * {@code option forwardfor} appends its own line instead of extending the client's one, so reading
     * only the first line would trust a value the client wrote.
     */
    private static String[] forwardedChain(HttpServletRequest request) {
        Enumeration<String> lines = request.getHeaders(FORWARDED_FOR);
        if (lines == null) {
            return new String[0];
        }
        List<String> hops = new ArrayList<>();
        while (lines.hasMoreElements()) {
            String line = lines.nextElement();
            if (line == null || line.isBlank()) {
                continue;
            }
            hops.addAll(Arrays.asList(line.split(",")));
        }
        return hops.toArray(new String[0]);
    }

    private boolean isTrusted(String address) {
        if (!isIpLiteral(address)) {
            return false;
        }
        for (IpAddressMatcher matcher : trustedProxies) {
            try {
                if (matcher.matches(address)) {
                    return true;
                }
            } catch (IllegalArgumentException e) {
                return false;
            }
        }
        return false;
    }

    static boolean isIpLiteral(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        Matcher v4 = IPV4.matcher(value);
        if (v4.matches()) {
            for (int group = 1; group <= 4; group++) {
                if (Integer.parseInt(v4.group(group)) > 255) {
                    return false;
                }
            }
            return true;
        }
        return value.indexOf(':') >= 0 && IPV6.matcher(value).matches();
    }
}
