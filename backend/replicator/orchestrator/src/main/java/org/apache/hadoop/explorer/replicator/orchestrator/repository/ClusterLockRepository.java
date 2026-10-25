package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.ClusterLockEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Optional;

@Repository
public interface ClusterLockRepository extends JpaRepository<ClusterLockEntity, String> {

    /**
     * Захват свободной аренды или продление своей одним атомарным UPDATE.
     *
     * <p>Эпоха растет только при смене владельца: продление своей же аренды сохраняет
     * выданный токен, иначе автопродление в {@code runWithLock} отсекало бы само себя.
     * Истекшей считается аренда с {@code lockedUntil <= now}, а не строго меньше: снятая
     * аренда помечается просроченной текущим моментом, и строгое сравнение сделало бы
     * повторный захват зависимым от разрешения часов БД.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE ClusterLockEntity l SET " +
           "l.lockedBy = :lockedBy, " +
           "l.lockedUntil = :lockedUntil, " +
           "l.updatedAt = :now, " +
           "l.epoch = CASE WHEN l.lockedBy = :lockedBy THEN l.epoch ELSE (l.epoch + 1) END " +
           "WHERE l.lockName = :lockName AND (l.lockedUntil <= :now OR l.lockedBy = :lockedBy)")
    int tryAcquireOrRenew(
        @Param("lockName") String lockName,
        @Param("lockedBy") String lockedBy,
        @Param("lockedUntil") Instant lockedUntil,
        @Param("now") Instant now
    );

    @Query("SELECT l.epoch FROM ClusterLockEntity l WHERE l.lockName = :lockName")
    Optional<Long> findEpochByLockName(@Param("lockName") String lockName);

    @Query("SELECT CASE WHEN count(l) > 0 THEN true ELSE false END FROM ClusterLockEntity l " +
           "WHERE l.lockName = :lockName AND l.lockedBy = :lockedBy AND l.epoch = :epoch AND l.lockedUntil > :now")
    boolean isLockHeldValid(
        @Param("lockName") String lockName,
        @Param("lockedBy") String lockedBy,
        @Param("epoch") long epoch,
        @Param("now") Instant now
    );

    /**
     * Снятие своей аренды без удаления строки: владелец обнуляется, срок помечается истекшим,
     * а накопленная эпоха остается в строке.
     *
     * <p>Удаление строки сбрасывало счетчик эпох: следующий захват начинался с единицы, и
     * отставший держатель с большим токеном прошлой аренды выглядел новее законного владельца —
     * ровно та ситуация, от которой ограждение по эпохе и защищает. Пустой {@code lockedBy}
     * не совпадает ни с одним идентификатором узла, поэтому следующий захват (в том числе тем же
     * узлом) пройдет по ветке смены владельца и получит новый, строго больший токен.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE ClusterLockEntity l SET " +
           "l.lockedBy = '', " +
           "l.lockedUntil = :now, " +
           "l.updatedAt = :now " +
           "WHERE l.lockName = :lockName AND l.lockedBy = :lockedBy")
    int releaseLock(@Param("lockName") String lockName,
                    @Param("lockedBy") String lockedBy,
                    @Param("now") Instant now);
}
