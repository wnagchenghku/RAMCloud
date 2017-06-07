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

class ProcedureManager {
  public:
    ProcedureManager();
    ~ProcedureManager();

    bool installProcedure(uint64_t tableId, Key key, TenantId tenantId,
            std::string runtimeType, Buffer* procedure);

    bool verifyProcedure(uint64_t tableId, Key key, TenantId tenantId,
            std::string runtimeType, Buffer* out=NULL);

    bool invokeProcedure(uint64_t tableId, Key key, TenantId tenantId,
            std::string runtimeType, Buffer* clientResponse,
            Buffer* binary=NULL);

    bool registerRuntime(std::string runtimeType);

  private:
    std::unordered_map<std::string, RuntimeFrontEnd*> runtimes;
    DISALLOW_COPY_AND_ASSIGN(ProcedureManager);
};

} // namespace

#endif // RAMCLOUD_PROCEDUREMANAGER_H
