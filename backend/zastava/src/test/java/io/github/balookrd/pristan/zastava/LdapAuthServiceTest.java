package io.github.balookrd.pristan.zastava;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.InMemoryListenerConfig;
import io.github.balookrd.pristan.zastava.auth.LdapAuthService;
import io.github.balookrd.pristan.zastava.config.ZastavaProperties;
import io.github.balookrd.pristan.zastava.model.Role;
import io.github.balookrd.pristan.zastava.model.UserSession;
import io.github.balookrd.pristan.zastava.security.RoleResolver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * LdapAuthService against an in-memory directory shaped like demo/infra/ldap/bootstrap.ldif:
 * groups are groupOfNames entries and users carry no memberOf attribute.
 */
class LdapAuthServiceTest {

    private static final String BASE = "dc=company,dc=local";

    private InMemoryDirectoryServer server;
    private ZastavaProperties properties;

    @BeforeEach
    void startDirectory() throws Exception {
        InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig(BASE);
        config.addAdditionalBindCredentials("cn=admin," + BASE, "adminpassword");
        config.setListenerConfigs(InMemoryListenerConfig.createLDAPConfig("default", 0));
        // memberOf is not in the standard schema; the test with memberOf needs it accepted
        config.setSchema(null);
        server = new InMemoryDirectoryServer(config);
        server.add("dn: " + BASE, "objectClass: domain", "dc: company");
        server.add("dn: ou=users," + BASE, "objectClass: organizationalUnit", "ou: users");
        server.add("dn: ou=groups," + BASE, "objectClass: organizationalUnit", "ou: groups");
        addUser("writer_user", "Владимир Инженеров");
        addUser("analyst_user", "Анна Аналитикова");
        addGroup("data-engineers", "writer_user");
        addGroup("yarn-operators", "writer_user");
        addGroup("bi-analysts", "analyst_user");
        server.startListening();

        properties = new ZastavaProperties();
        var ldap = properties.getLdap();
        ldap.setEnabled(true);
        ldap.setUrl("ldap://127.0.0.1:" + server.getListenPort());
        ldap.setManagerDn("cn=admin," + BASE);
        ldap.setManagerPassword("adminpassword");
        ldap.setUserSearchBase("ou=users," + BASE);
        ldap.setUserSearchFilter("(&(objectClass=inetOrgPerson)(uid={0}))");
        ldap.setGroupSearchBase("ou=groups," + BASE);
        ldap.setGroupSearchFilter("(&(objectClass=groupOfNames)(member={0}))");
    }

    @AfterEach
    void stopDirectory() {
        server.shutDown(true);
    }

    @Test
    @DisplayName("Without memberOf, groups come from group-search-filter and drive the role")
    void resolvesGroupsBySearchWhenMemberOfIsAbsent() {
        Optional<UserSession> session = service().authenticate("writer_user", "password123");

        assertThat(session).isPresent();
        assertThat(session.get().groups()).containsExactlyInAnyOrder("data-engineers", "yarn-operators");
        assertThat(session.get().systemRole()).isEqualTo(Role.WRITER);
        assertThat(session.get().authMethod()).isEqualTo("ldap");
    }

    @Test
    @DisplayName("memberOf, when present, is used as is")
    void prefersMemberOf() throws Exception {
        server.modify("dn: uid=analyst_user,ou=users," + BASE, "changetype: modify",
                "add: memberOf", "memberOf: cn=analytics,ou=groups," + BASE);

        Optional<UserSession> session = service().authenticate("analyst_user", "password123");

        assertThat(session).isPresent();
        assertThat(session.get().groups()).containsExactly("analytics");
    }

    @Test
    @DisplayName("A failing group search keeps the verified login, without groups")
    void groupSearchFailureDoesNotFailLogin() {
        properties.getLdap().setGroupSearchBase("ou=missing," + BASE);

        Optional<UserSession> session = service().authenticate("writer_user", "password123");

        assertThat(session).isPresent();
        assertThat(session.get().groups()).isEmpty();
        assertThat(session.get().systemRole()).isEqualTo(Role.READER);
    }

    @Test
    @DisplayName("Wrong password is rejected")
    void rejectsWrongPassword() {
        assertThat(service().authenticate("writer_user", "wrong")).isEmpty();
    }

    private LdapAuthService service() {
        return new LdapAuthService(properties, new RoleResolver(properties));
    }

    private void addUser(String uid, String displayName) throws Exception {
        server.add("dn: uid=" + uid + ",ou=users," + BASE,
                "objectClass: inetOrgPerson",
                "uid: " + uid,
                "cn: " + uid,
                "sn: " + uid,
                "displayName: " + displayName,
                "userPassword: password123");
    }

    private void addGroup(String cn, String memberUid) throws Exception {
        server.add("dn: cn=" + cn + ",ou=groups," + BASE,
                "objectClass: groupOfNames",
                "cn: " + cn,
                "member: uid=" + memberUid + ",ou=users," + BASE);
    }
}
