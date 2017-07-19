#ifndef RAMCLOUD_RUNTIMEFRONTEND_H
#define RAMCLOUD_RUNTIMEFRONTEND_H

#include <memory>

#include "Util.h"
#include "Buffer.h"

#include "TenantId.h"
#include "RuntimeBackEnd.h"

namespace RAMCloud {

/**
 * An interface to install and invoke extensions/procedures in RAMCloud.
 *
 * Implementations allow tenants to install/compile and invoke untrusted code
 * written for (in) a specific runtime (language) inside RAMCloud.
 */
class RuntimeFrontEnd {
  protected:
    /**
     * Constructor for RuntimeFrontEnd.
     */
    RuntimeFrontEnd(int workerId, std::shared_ptr<RuntimeBackEnd> backend)
        : workerId(workerId)
        , backend(backend)
    {}

    /// The id of the worker the frontend belongs to.
    int workerId;

    /// A shared pointer to a backend for the runtime.
    std::shared_ptr<RuntimeBackEnd> backend;

  public:
    /**
     * Destructor for RuntimeFrontEnd.
     */
    virtual ~RuntimeFrontEnd() {}

    /**
     * Install/Compile a procedure and return a version that can be invoked.
     *
     * \param tableId
     *      The table the procedure will be added to. A few runtimes (ex: V8)
     *      might need the tableId for internal management of runtime state.
     * \param key
     *      A key identifying the procedure. A few runtimes (ex: V8) might need
     *      this key for internal management of runtime state.
     * \param tenantId
     *      The tenant installing the procedure. Required primarily for
     *      resource accounting. Additionally, a few runtimes (ex: V8) might
     *      need the tenantId for internal management of runtime state.
     * \param procedure
     *      A pointer to a Buffer containing the procedure to be installed.
     *
     * \param[out] binary
     *      A pointer to a Buffer which will be appended with a version of the
     *      binary that can be invoked.
     */
    virtual void installProcedure(uint64_t tableId, Key& key, TenantId tenantId,
                                  Buffer* procedure, Buffer* binary) = 0;

    /**
     * Invoke a procedure and return the results.
     *
     * \param tableId
     *      The table the procedure belongs to. A few runtimes (ex: V8) might
     *      need the tableId for internal management of runtime state.
     * \param key
     *      A key identifying the procedure. A few runtimes (ex: V8) might need
     *      this key for internal management of runtime state.
     * \param tenantId
     *      The tenant invoking the procedure. Required primarily for resource
     *      accounting. Additionally, a few runtimes (ex: V8) might need the
     *      tenantId for internal management of runtime state.
     * \param binary
     *      A pointer to a Buffer containing the procedure in a format that can
     *      directly invoked/executed.
     *
     * \param[out] clientResponse
     *      A pointer to a Buffer into which the result/response of the
     *      procedure will be appended.
     */
    virtual void invokeProcedure(uint64_t tableId, Key& key, TenantId tenantId,
                                 Buffer* binary, Buffer* clientResponse) = 0;

  private:
    DISALLOW_COPY_AND_ASSIGN(RuntimeFrontEnd);
};

} // namespace

#endif // RAMCLOUD_RUNTIMEFRONTEND_H
