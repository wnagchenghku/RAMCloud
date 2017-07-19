#ifndef RAMCLOUD_PROCEDUREMANAGER_H
#define RAMCLOUD_PROCEDUREMANAGER_H

#include <string>
#include <unordered_map>

#include "Key.h"
#include "Util.h"
#include "Buffer.h"

#include "TenantId.h"
#include "RuntimeFrontEnd.h"

namespace RAMCloud {

/**
 * This enum class helps determine the runtime a procedure should be invoked or
 * installed on. It is used by ProcedureManager to identify the frontend a given
 * request should be dispatched to.
 */
enum class RuntimeType : uint8_t {
    NATIVE,
    INVALID
};

/**
 * This struct is required by std::unordered_map to be able to hash RuntimeType
 * to a RuntimeFrontEnd and a RuntimeBackEnd. See FrontendMap and BackendMap
 * below.
 */
struct RuntimeTypeHash {
    std::size_t operator()(const RuntimeType& runtimeType) const
    {
        return std::hash<uint8_t>()(static_cast<uint8_t>(runtimeType));
    }
};

/**
 * This "type" defines an std::unordered_map which allows for a core-local
 * frontend to be looked up given a runtime.
 */
typedef std::unordered_map<RuntimeType, std::unique_ptr<RuntimeFrontEnd>,
        RuntimeTypeHash> FrontendMap;

/**
 * This "type" defines an std::unordered_map which allows for a shared backend
 * to be looked up given a runtime.
 */
typedef std::unordered_map<RuntimeType, std::shared_ptr<RuntimeBackEnd>,
        RuntimeTypeHash> BackendMap;

/**
 * Currently manages runtimes and helps dispatch requests to the appropriate
 * runtime. There is one instance of this class associated with every worker
 * thread.
 *
 * On receiving a request to invoke or install a procedure, MasterService
 * calls into the worker-local instance of this class. This class then
 * dispatches the request to the appropriate worker-local frontend based on
 * the request's runtime. installProcedure() is called when a procedure has
 * to be "pushed" into RAMCloud. invokeProcedure() is called when a previously
 * pushed procedure needs to be executed inside RAMCloud.
 *
 * This class initializes and registers runtimes in a lazy manner i.e a
 * runtime frontend is created when the worker thread receives it's first
 * request for that runtime. As a result, different worker threads can very
 * well have a different set of active runtimes at any instant of time.
 *
 * For more info on runtimes, refer to classes RuntimeFrontEnd and
 * RuntimeBackEnd.
 */
class ProcedureManager {
  public:
    ProcedureManager(SpinLock* backendsLock, BackendMap* backends);
    ~ProcedureManager();

    void installProcedure(uint64_t tableId, Key& key, TenantId tenantId,
                          RuntimeType runtimeType, Buffer* procedure);
    void invokeProcedure(uint64_t tableId, Key& key, TenantId tenantId,
                         RuntimeType runtimeType, Buffer* clientResponse);

    static RuntimeType getRuntimeTypeFromString(std::string& runtimeType);

  private:
    void verifyProcedure(uint64_t tableId, Key& key, TenantId tenantId,
                         RuntimeType runtimeType, Buffer* out=nullptr);
    void registerRuntime(RuntimeType runtimeType);
    std::shared_ptr<RuntimeBackEnd> createBackEnd(RuntimeType runtimeType);
    void createFrontEnd(RuntimeType runtimeType,
                        std::shared_ptr<RuntimeBackEnd> backend);

    /// A map of active frontends (of type RuntimeFrontEnd) on this core.
    FrontendMap frontends;

    /// Required to safely access backends.
    std::shared_ptr<SpinLock> backendsLock;

    /// A map of active backends on this master. This map is shared across all
    /// this master's cores.
    std::shared_ptr<BackendMap> backends;

    DISALLOW_COPY_AND_ASSIGN(ProcedureManager);
};

} // namespace

#endif // RAMCLOUD_PROCEDUREMANAGER_H
