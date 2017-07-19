#include "ClientException.h"

#include "NativeBackEnd.h"

namespace RAMCloud {

void
NativeBackEnd::putProcedure(Key& key, TenantId tenantId, void* procedure)
{
    backendLock.lock();

    std::string keyStr(reinterpret_cast<const char*>(key.getStringKey()),
            key.getStringKeyLength());
    if (procedureMap.find(keyStr) != procedureMap.end()) {
        backendLock.unlock();
        ClientException::throwException(HERE, STATUS_INTERNAL_ERROR);
    }
    procedureMap[keyStr] = procedure;

    backendLock.unlock();
}

void*
NativeBackEnd::getProcedure(Key& key, TenantId tenantId)
{
    backendLock.lock();

    std::string keyStr(reinterpret_cast<const char*>(key.getStringKey()),
            key.getStringKeyLength());
    auto procedure = procedureMap.find(keyStr);

    if (procedure == procedureMap.end()) {
        backendLock.unlock();
        ClientException::throwException(HERE, STATUS_INTERNAL_ERROR);
    }

    backendLock.unlock();

    return procedure->second;
}

} // namespace
