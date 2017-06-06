#ifndef RAMCLOUD_PROCEDUREMANAGER_H
#define RAMCLOUD_PROCEDUREMANAGER_H

namespace RAMCloud {

class ProcedureManager {
  public:
    ProcedureManager();
    ~ProcedureManager();

    bool invoke(Key key, TenantId tenantId, Buffer* request, Buffer* response,
            std::string runtime);

    bool registerRuntime(std::string runtime);

  private:
    std::unordered_map<std::string, RuntimeFrontEnd*> runtimes;
    DISALLOW_COPY_AND_ASSIGN(ProcedureManager);
};

} // namespace

#endif // RAMCLOUD_PROCEDUREMANAGER_H
