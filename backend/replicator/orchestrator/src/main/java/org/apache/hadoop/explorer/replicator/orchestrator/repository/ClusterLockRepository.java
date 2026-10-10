package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.ClusterLockEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;

@Repository
public interface ClusterLockRepository extends JpaRepository<ClusterLockEntity, String> {

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE ClusterLockEntity l SET l.lockedBy = :lockedBy, l.lockedUntil = :lockedUntil, l.updatedAt = :now " +
           "WHERE l.lockName = :lockName AND (l.lockedUntil < :now OR l.lockedBy = :lockedBy)")
    int tryAcquireOrRenew(
        @Param("lockName") String lockName,
        @Param("lockedBy") String lockedBy,
        @Param("lockedUntil") Instant lockedUntil,
        @Param("now") Instant now
    );

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("DELETE FROM ClusterLockEntity l WHERE l.lockName = :lockName AND l.lockedBy = :lockedBy")
    int releaseLock(@Param("lockName") String lockName, @Param("lockedBy") String lockedBy);
}
