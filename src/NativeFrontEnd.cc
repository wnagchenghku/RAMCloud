#include <fcntl.h>
#include <dlfcn.h>

#include "Common.h"
#include "ClientException.h"

#include "NativeFrontEnd.h"

namespace RAMCloud {

void
NativeFrontEnd::installProcedure(uint64_t tableId, Key& key, TenantId tenantId,
        Buffer* procedure, Buffer* binary)
{
    int procFd = ::open("./tmpNative", O_RDWR | O_CREAT | O_TRUNC,
            S_IRWXU | S_IRWXG | S_IRWXO);
    ::write(procFd, procedure->getRange(0, procedure->size()),
            procedure->size());
    ::close(procFd);

    void* procHandle = ::dlopen("./tmpNative", RTLD_LAZY);
    if (procHandle == nullptr) {
        RAMCLOUD_LOG(ERROR, "%s", dlerror());
        ClientException::throwException(HERE, STATUS_INTERNAL_ERROR);
    }

    std::static_pointer_cast<NativeBackEnd>(backend)->putProcedure(
            key, tenantId, procHandle);
}

void
NativeFrontEnd::invokeProcedure(uint64_t tableId, Key& key, TenantId tenantId,
        Buffer* binary, Buffer* clientResponse)
{
    void* procHandle =
            std::static_pointer_cast<NativeBackEnd>(backend)->getProcedure(
            key, tenantId);

    int (*invoke)(int) = nullptr;
    *(void **)(&invoke) = ::dlsym(procHandle, "procedureInvoke");

    if (invoke == nullptr) {
        RAMCLOUD_LOG(ERROR, "%s", dlerror());
    }

    RAMCLOUD_LOG(NOTICE, "%d", (*invoke)(11));

    ::dlclose(procHandle);
}

}
