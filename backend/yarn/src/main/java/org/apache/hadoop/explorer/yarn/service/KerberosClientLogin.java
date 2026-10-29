package org.apache.hadoop.explorer.yarn.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.explorer.yarn.config.YarnProperties;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Keytab login of the explorer's own Kerberos identity, used for SPNEGO calls to secured
 * ResourceManagers. The login happens lazily, on the first call to a Kerberos-enabled cluster,
 * and the TGT is renewed from the keytab before every use.
 */
@Component
public class KerberosClientLogin {

    private static final Logger log = LoggerFactory.getLogger(KerberosClientLogin.class);

    private final YarnProperties.KerberosClientConfig config;
    private UserGroupInformation ugi;

    public KerberosClientLogin(YarnProperties yarnProperties) {
        this.config = yarnProperties.getKerberos();
    }

    public synchronized UserGroupInformation ugi() throws IOException {
        if (ugi == null) {
            if (isBlank(config.getPrincipal()) || isBlank(config.getKeytab())) {
                throw new IOException("Кластер помечен kerberos-enabled, но не заданы yarn.kerberos.principal "
                        + "и yarn.kerberos.keytab — учетная запись Explorer для обращения к ResourceManager");
            }
            if (!isBlank(config.getKrb5Conf())) {
                System.setProperty("java.security.krb5.conf", config.getKrb5Conf());
            }
            Configuration conf = new Configuration(false);
            conf.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(conf);
            ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(config.getPrincipal(), config.getKeytab());
            log.info("Kerberos login for outbound ResourceManager calls: {}", config.getPrincipal());
        } else {
            ugi.checkTGTAndReloginFromKeytab();
        }
        return ugi;
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
