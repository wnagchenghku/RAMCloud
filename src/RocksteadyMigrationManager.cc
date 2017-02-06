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
    // TODO: Iterate through the set of in-progress migrations.
    // Make sure all of them complete, delete them, and only then
    // return from this method.
    ;
}

int
RocksteadyMigrationManager::poll()
{
    return 0;
}

bool
RocksteadyMigrationManager::startMigration(ServerId sourceServerId,
                            uint64_t tableId,
                            uint64_t startKeyHash,
                            uint64_t endKeyHash)
{
    return true;
}

}  // namespace RAMCloud
