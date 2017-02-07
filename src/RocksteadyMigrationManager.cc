#include <new>

#include "RocksteadyMigrationManager.h"

namespace RAMCloud {

RocksteadyMigrationManager::RocksteadyMigrationManager(Context* context)
    : Dispatch::Poller(context->dispatch, "RocksteadyMigrationManager")
    , context(context)
    , migrationsInProgress()
{
    ;
}

RocksteadyMigrationManager::~RocksteadyMigrationManager()
{
    // Iterate through the set of in-progress migrations and make sure all
    // of them complete, delete them, and only then return from this method.
    for (auto migration = migrationsInProgress.begin();
        migration != migrationsInProgress.end(); ) {

        while ((*migration)->phase != RocksteadyMigration::COMPLETED) {
            (*migration)->poll();
        }

        RocksteadyMigration* completedMigration = *migration;
        migration = migrationsInProgress.erase(migration);
        delete completedMigration;
    }
}

int
RocksteadyMigrationManager::poll()
{
    int workPerformed = 0;

    for (auto migration = migrationsInProgress.begin();
        migration != migrationsInProgress.end(); ) {

        workPerformed += (*migration)->poll();

        // Delete any completed migrations.
        if ((*migration)->phase == RocksteadyMigration::COMPLETED) {
            RocksteadyMigration* completedMigration = *migration;
            migration = migrationsInProgress.erase(migration);
            delete completedMigration;
        } else {
            migration++;
        }
    }

    return workPerformed;
}

bool
RocksteadyMigrationManager::startMigration(ServerId sourceServerId,
                            uint64_t tableId, uint64_t startKeyHash,
                            uint64_t endKeyHash)
{
    // First check if we have another in-progress migration from the
    // same source.
    for (auto& migration : migrationsInProgress) {
        if (migration->sourceServerId == sourceServerId) {
            return false;
        }
    }

    // Add the new migration to the manager.
    RocksteadyMigration* newMigration = new RocksteadyMigration(context,
                                        sourceServerId, tableId,
                                        startKeyHash, endKeyHash);

    // XXX Should I synchronize here?
    migrationsInProgress.push_back(newMigration);

    return true;
}

RocksteadyMigration::RocksteadyMigration(Context* context,
                        ServerId sourceServerId, uint64_t tableId,
                        uint64_t startKeyHash, uint64_t endKeyHash)
    : context(context)
    , sourceServerId(sourceServerId)
    , tableId(tableId)
    , startKeyHash(startKeyHash)
    , endKeyHash(endKeyHash)
    , phase(RocksteadyMigration::SETUP)
{
    ;
}

int
RocksteadyMigration::poll()
{
    return 0;
}

}  // namespace RAMCloud
