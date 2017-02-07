#ifndef RAMCLOUD_ROCKSTEADYMIGRATIONMANAGER_H
#define RAMCLOUD_ROCKSTEADYMIGRATIONMANAGER_H

#include <deque>

#include "Dispatch.h"
#include "ServerId.h"

namespace RAMCloud {

// Forward declaration.
class RocksteadyMigration;

class RocksteadyMigrationManager : Dispatch::Poller {
  public:
    explicit RocksteadyMigrationManager(Context* context);
    ~RocksteadyMigrationManager();

    int poll();
    bool startMigration(ServerId sourceServerId, uint64_t tableId,
                uint64_t startKeyHash, uint64_t endKeyHash);

  PRIVATE:
    Context* context;

    std::deque<RocksteadyMigration*> migrationsInProgress;

    DISALLOW_COPY_AND_ASSIGN(RocksteadyMigrationManager);
};

class RocksteadyMigration {
  public:
    explicit RocksteadyMigration(Context* context, ServerId sourceServerId,
            uint64_t tableId, uint64_t startKeyHash, uint64_t endKeyHash);
    ~RocksteadyMigration() {}

    int poll();

  PRIVATE:
    Context* context;

    enum MigrationPhase {
        SETUP,

        MIGRATING_DATA,

        TEAR_DOWN,

        COMPLETED
    };

    ServerId sourceServerId;

    uint64_t tableId;

    uint64_t startKeyHash;

    uint64_t endKeyHash;

    MigrationPhase phase;

    friend class RocksteadyMigrationManager;
    DISALLOW_COPY_AND_ASSIGN(RocksteadyMigration);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_ROCKSTEADYMIGRATIONMANAGER_H
