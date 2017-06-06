#ifndef RAMCLOUD_TENANTID_H
#define RAMCLOUD_TENANTID_H

namespace RAMCloud {

class TenantId {
  public:
    TenantId(uint64_t id)
        : id(id)
    {}
    ~TenantId() {}

    uint64_t
    getId()
    {
        return id;
    }

  private:
    uint64_t id;
};

} // namespace

#endif // RAMCLOUD_TENANTID_H
