#include <utility>

#include "ProcedureManager.h"

namespace RAMCloud {

ProcedureManager::ProcedureManager()
    : runtimes()
{
    // Reserve space for sixteen runtimes upfront. It is unlikely that a master
    // will have to support more.
    runtimes.reserve(16);
}

ProcedureManager::~ProcedureManager()
{
    // Delete all runtimes.
    for (auto frontEnd = runtimes.begin(); frontEnd != runtimes.end();) {
        delete frontEnd->second;
        frontEnd = runtimes.erase(frontEnd);
    }
}

/**
 * Invokes the appropriate frontend to compile the client supplied procedure,
 * and writes it to the log.
 */
void
ProcedureManager::installProcedure(uint64_t tableId, Key key,
        TenantId tenantId, std::string runtimeType, Buffer* procedure)
{
    ;
}

/**
 * Invoke a procedure on this master.
 *
 * All results are appended to the clientResponse Buffer.
 */
void
ProcedureManager::invokeProcedure(uint64_t tableId, Key key, TenantId tenantId,
        std::string runtime, Buffer* clientResponse, Buffer* binary)
{
    ;
}

/**
 * Verifies that the information associated with the procedure in the log
 * matches that with which it is being invoked by the client.
 *
 * On successful verification, the Buffer pointed to by out will contain the
 * procedure.
 */
void
ProcedureManager::verifyProcedure(uint64_t tableId, Key key, TenantId tenantId,
        std::string runtimeType, Buffer* out)
{
    ;
}

/**
 * Register a runtime with the procedure-manager.
 */
void
ProcedureManager::registerRuntime(std::string runtimeType)
{
    // Check if the runtime has already been registered with the
    // procedure-manager.
    if (runtimes.find(runtimeType) != runtimes.end()) {
        RAMCLOUD_LOG(WARNING, "Attempted to re-register runtime of type"
                "%s with the procedure-manager.", runtimeType.c_str());

        return;
    }

    // TODO: Insert a frontEnd into runtimes.

    return;
}

} // namespace
