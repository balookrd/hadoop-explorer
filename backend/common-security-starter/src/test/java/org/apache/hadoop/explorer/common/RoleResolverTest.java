package org.apache.hadoop.explorer.common;

import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.common.model.Role;
import org.apache.hadoop.explorer.common.security.RoleResolver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RoleResolverTest {

    @Test
    @DisplayName("Должен распознавать системного администратора по стандартной группе")
    void shouldResolveAdminByDefaultGroup() {
        RoleResolver resolver = new RoleResolver(new CommonSecurityProperties());

        var res1 = resolver.resolve("john", List.of("hadoop-admins"));
        assertEquals(Role.ADMIN, res1.role());
        assertTrue(res1.isAdmin());

        var res2 = resolver.resolve("ALICE", List.of("SuperUsers"));
        assertEquals(Role.ADMIN, res2.role());
        assertTrue(res2.isAdmin());
    }

    @Test
    @DisplayName("Должен распознавать роль писателя (Writer) по группе инженеров данных")
    void shouldResolveWriterByDefaultGroup() {
        RoleResolver resolver = new RoleResolver(new CommonSecurityProperties());

        var res = resolver.resolve("bob", List.of("data-engineers"));
        assertEquals(Role.WRITER, res.role());
        assertFalse(res.isAdmin());
    }

    @Test
    @DisplayName("Должен назначать роль читателя (Reader) по умолчанию для обычных пользователей")
    void shouldResolveReaderByDefault() {
        RoleResolver resolver = new RoleResolver(new CommonSecurityProperties());

        var res = resolver.resolve("guest", List.of("analytics", "viewers"));
        assertEquals(Role.READER, res.role());
        assertFalse(res.isAdmin());
    }

    @Test
    @DisplayName("Должен учитывать кастомную конфигурацию admin-пользователей и групп")
    void shouldRespectCustomConfig() {
        CommonSecurityProperties props = new CommonSecurityProperties();
        props.getAuth().setAdminUsers(Set.of("super-root"));
        props.getAuth().setWriterGroups(Set.of("custom-devs"));

        RoleResolver resolver = new RoleResolver(props);

        var adminRes = resolver.resolve("super-root", List.of());
        assertEquals(Role.ADMIN, adminRes.role());
        assertTrue(adminRes.isAdmin());

        var writerRes = resolver.resolve("dave", List.of("custom-devs"));
        assertEquals(Role.WRITER, writerRes.role());
        assertFalse(writerRes.isAdmin());
    }
}
