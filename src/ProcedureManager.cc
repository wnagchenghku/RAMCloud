#include <utility>

#include "ThreadId.h"
#include "ClientException.h"

#include "NativeBackEnd.h"
#include "NativeFrontEnd.h"
#include "ProcedureManager.h"

namespace RAMCloud {

ProcedureManager::ProcedureManager(SpinLock* backendsLock, BackendMap* backends)
    : frontends()
    , backendsLock(backendsLock)
    , backends(backends)
{
    // Reserve space for sixteen runtimes upfront. It is unlikely that a master
    // will have to support more.
    frontends.reserve(16);
}

ProcedureManager::~ProcedureManager()
{
    // Delete all runtimes.
    for (auto frontEnd = frontends.begin(); frontEnd != frontends.end();) {
        (frontEnd->second).reset();
        frontEnd = frontends.erase(frontEnd);
    }
}

/**
 * Identifies the appropriate frontend to compile the client supplied procedure,
 * and writes it to the log (TODO).
 *
 * \param tableId
 *      The table the procedure should be added to.
 * \param key
 *      The key associated with the procedure. Required to identify the
 *      procedure on future invocations.
 * \param tenantId
 *      The tenant invoking the procedure. Required for resource accounting
 *      and verification on future invocations.
 * \param runtimeType
 *      The runtime to install the procedure on.
 * \param procedure
 *      A pointer to the Buffer containing the actual procedure to be installed.
 */
void
ProcedureManager::installProcedure(uint64_t tableId, Key& key,
        TenantId tenantId, RuntimeType runtimeType, Buffer* procedure)
{
    Buffer binary;

    auto frontendIt = frontends.find(runtimeType);
    if (frontendIt == frontends.end()) {
        // Slow Path:
        // This runtime has not been registered on this worker yet. Attempt to
        // do so. There will be two additional accesses to frontends under this
        // path, one of which will be performed inside the call to
        // registerRuntime().
        registerRuntime(runtimeType);
        frontends[runtimeType]->installProcedure(tableId, key, tenantId,
                procedure, &binary);
    } else {
        // Fast Path:
        // No additional accesses to frontends.
        frontendIt->second->installProcedure(tableId, key, tenantId, procedure,
                &binary);
    }

    // TODO: Write binary to the log.
}

/**
 * Invoke a procedure after identifying the appropriate frontend. All results
 * are appended to the clientResponse Buffer by the appropriate frontend.
 *
 * \param tableId
 *      The table the procedure belongs to. Required to identify the procedure.
 * \param key
 *      The key associated with the procedure. Required to identify the
 *      procedure.
 * \param tenantId
 *      The tenant invoking the procedure. Required for resource accounting
 *      and verification.
 * \param runtimeType
 *      The runtime to invoke the procedure on.
 *
 * \param[out] clientResponse
 *      A pointer to a Buffer into which the invoked procedure will append
 *      the result/response.
 */
void
ProcedureManager::invokeProcedure(uint64_t tableId, Key& key, TenantId tenantId,
        RuntimeType runtimeType, Buffer* clientResponse)
{
    Buffer binary;
    verifyProcedure(tableId, key, tenantId, runtimeType, &binary);

    auto frontendIt = frontends.find(runtimeType);
    if (frontendIt == frontends.end()) {
        // Slow Path:
        // This runtime has not been registered on this worker yet, but the
        // procedure was successfully verified. Attempt to do so. There will
        // be two additional accesses to frontends under this path, one of
        // which will be performed inside the call to registerRuntime().
        registerRuntime(runtimeType);
        frontends[runtimeType]->invokeProcedure(tableId, key, tenantId,
                clientResponse, &binary);
    } else {
        // Fast Path:
        // No additional accesses to frontends.
        frontendIt->second->invokeProcedure(tableId, key, tenantId,
                clientResponse, &binary);
    }
}

/**
 * Convert the runtime-type from a C++ string to the RuntimeType enum.
 *
 * \param runtimeType
 *      The runtime type in the C++ string format.
 *
 * \throw InternalError
 *      The given runtime type is not supported by RAMCloud.
 */
RuntimeType
ProcedureManager::getRuntimeTypeFromString(std::string& runtimeType)
{
    if (runtimeType == "native") {
        return RuntimeType::NATIVE;
    } else {
        ClientException::throwException(HERE, STATUS_INTERNAL_ERROR);
    }

    // For compilation. Control should *never* reach here.
    return RuntimeType::INVALID;
}

/**
 * Verifies that the information associated with the procedure in the log
 * matches that with which it is being invoked by the client.
 *
 * On successful verification, the Buffer pointed to by out will contain the
 * procedure.
 */
void
ProcedureManager::verifyProcedure(uint64_t tableId, Key& key, TenantId tenantId,
        RuntimeType runtimeType, Buffer* out)
{
    ;
}

/**
 * Register a runtime with the procedure-manager.
 */
void
ProcedureManager::registerRuntime(RuntimeType runtimeType)
{
    backendsLock->lock();
    auto backendIt = backends->find(runtimeType);
    // If a backend does not exist, create one.
    auto backend = (backendIt == backends->end()) ?
            createBackEnd(runtimeType) : backendIt->second;
    backendsLock->unlock();

    createFrontEnd(runtimeType, backend);
}

/**
 * Creates a backend for runtimeType that will be shared across cores.
 */
std::shared_ptr<RuntimeBackEnd>
ProcedureManager::createBackEnd(RuntimeType runtimeType)
{
    switch (runtimeType) {
    // Create a backend for the native runtime.
    case RuntimeType::NATIVE: {
        auto nativeBackend =
                std::shared_ptr<NativeBackEnd>(new NativeBackEnd());
        if (nativeBackend == nullptr) {
            // Failed to create backend.
            ClientException::throwException(HERE, STATUS_INTERNAL_ERROR);
        } else {
            auto backend =
                    std::static_pointer_cast<RuntimeBackEnd>(nativeBackend);
            (*backends)[runtimeType] = backend;
            return backend;
        }
        break;
    }

    // Unrecognized runtime.
    default:
        ClientException::throwException(HERE, STATUS_INTERNAL_ERROR);
        break;
    }

    // For compilation. Control should *never* reach this statement.
    return nullptr;
}

/**
 * Creates a core-local frontend for runtimeType.
 */
void
ProcedureManager::createFrontEnd(RuntimeType runtimeType,
        std::shared_ptr<RuntimeBackEnd> backend)
{
    switch (runtimeType) {
    // Create a frontend for the native runtime.
    case RuntimeType::NATIVE: {
        auto frontend = std::unique_ptr<NativeFrontEnd>(
                new NativeFrontEnd(ThreadId::get(), backend));
        if (frontend == nullptr) {
            // Failed to create a frontend for the native runtime.
            ClientException::throwException(HERE, STATUS_INTERNAL_ERROR);
        } else {
            frontends[runtimeType] = std::move(frontend);
        }
        break;
    }

    // Unrecognized runtime.
    default:
        ClientException::throwException(HERE, STATUS_INTERNAL_ERROR);
        break;
    }
}

} // namespace
