/* Copyright (c) 2017 University of Utah
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "ClientException.h"
#include "OptionParser.h"
#include "RamCloud.h"

using namespace RAMCloud;

// TODO
// - Need something like htonl on ids.

static constexpr char const* nextIdKey = "__nextId";

size_t constexpr static_strlen(const char* str)
{
    return *str ? 1 + static_strlen(str + 1) : 0;
}

class TAO {
  public:

    typedef uint16_t OType;
    typedef uint16_t AType;
    typedef uint64_t Id;
    typedef uint32_t Time;

    struct KVSet {};

    struct ObjectHeader {
        ObjectHeader(OType otype)
            : otype{otype}
        {}
        OType otype;
    } __attribute__((__packed__));;

    struct AssocList {
      private:
        struct Assoc {
            Assoc()
                : id{}
                , time{}
            {}

            Assoc(Id id, Time time)
                : id{id}
                , time{time}
            {}

            Id id;
            Time time;

            bool operator<(const Assoc& other) {
                return id < other.id;
            }
        } __attribute__((__packed__));

        Assoc& at(uint32_t i) {
            return *buffer->getOffset<Assoc>(i);
        }

      public:

        /**
         * \param buffer
         *   Buffer that contains a value create by AssocList::serialize().
         */
        AssocList(Buffer* buffer)
            : buffer{buffer}
        {
        }


        uint32_t size() {
            return downCast<uint32_t>(buffer->size() / sizeof(Assoc));
        }

        void remove(Id id2) {
            for (uint32_t i = 0; i < size(); ++i) {
                if (at(i).id == id2) {
                    for (; i < size() - 1; ++i) {
                        at(i) = at(i + 1);
                    }
                    buffer->truncate(buffer->size() -
                                     downCast<uint32_t>(sizeof(Assoc)));
                    return;
                }
            }
        }

        void add(Id id2, Time time) {
            buffer->emplacePrepend<Assoc>(id2, time);
            for (uint32_t i = 1; i < size(); ++i) {
                if (at(i - 1).time <= at(i).time)
                    break;
                std::swap(at(i - 1), at(i));
            }
        }

        /**
         * \param id2s
         *      A set of Ids to test for in this lists. Ids are rearranged
         *      within this array, and the value at \a nId2s is adjusted
         *      to describe which prefix of the values in the array are
         *      represented in this list.
         */
        void filter(Id* id2s, uint32_t* nId2s) {
            for (uint32_t j = 0; j < *nId2s; ++j) {
                for (uint32_t i = 0; i < size(); ++i) {
                    if (id2s[j] == at(i).id) {
                        std::swap(id2s[j], id2s[*nId2s - 1]);
                        --(*nId2s);
                        break;
                    }
                }
            }
            return;
        }

        Buffer* buffer;
    };

    struct AssocListKey {
        Id id1;
        AType atype;
    } __attribute__((__packed__));;

    struct AssocKey {
        Id id1;
        AType atype;
        Id id2;
    } __attribute__((__packed__));;

    Id allocId() {
        constexpr bool LOCAL_ID_ALLOC = false;
        if (LOCAL_ID_ALLOC) {
            return nextId++;
        } else {
            return client->incrementInt64(
                       objectTableId,
                       nextIdKey,
                       downCast<uint16_t>(static_strlen(nextIdKey)), 1);
        }
    }

    TAO(RamCloud* client)
        : client{client}
        , objectTableId{}
        , assocTableId{}
        , nextId{}
    {
        initialize();
    }

    /**
     * obj_add(atype, (k -> v)*): creates new object, returns its id.
     * NOTE: Destructive to \a kvpairs.
     */
    Id objectAdd(OType otype, Buffer& kvpairs) {
        Id id = allocId();
        kvpairs.emplacePrepend<ObjectHeader>(otype);
        client->write(objectTableId,
                      &id, downCast<uint16_t>(sizeof(id)),
                      kvpairs.getRange(0, kvpairs.size()),
                      kvpairs.size());
        return id;
    }

    /**
     * obj_update(id, (k -> v)*): updates fields.
     * NOTE: Leaves logical contents of \a kvpairs alone, but will cause it to be
     * flattened into a single contiguous chunk.
     *
     */
    void objectUpdate(Id id, Buffer& kvpairs) {
        client->write(objectTableId,
                      &id, downCast<uint16_t>(sizeof(id)),
                      kvpairs.getRange(0, kvpairs.size()),
                      kvpairs.size());
    }

    /// obj_delete(id): removes the object permanently
    void objectDelete(Id id) {
        client->remove(objectTableId, &id, downCast<uint16_t>(sizeof(id)));
    }

    /**
     * obj_get(id): returns type and fields of an object.
     * NOTE: Destructive to \a kvpairs.
     */
    OType objectGet(uint64_t id, Buffer& kvpairs) {
        client->read(objectTableId,
                     &id, downCast<uint16_t>(sizeof(id)), &kvpairs);
        ObjectHeader* header = kvpairs.getStart<ObjectHeader>();
        kvpairs.truncateFront(sizeof(header));
        return header->otype;
    }

    /**
     * assoc_add(id1, atype, id2, time, (k -> v)*): adds or overwrites updates the
     * the association (id1, atype,id2), and its inverse (id1, inv(atype), id2)
     * if defined.
     * NOTE: Flattens kvpairs.
     */
    void assocAdd(Id id1, AType atype, Id id2,
                  Time time, Buffer& kvpairs)
    {
        try {
           RejectRules rejectRules{0, 0, 0, 0, 0};
           rejectRules.exists = 1;
           AssocKey kvpairsKey{id1, atype, id2};
           client->write(assocTableId,
                         &kvpairsKey, downCast<uint16_t>(sizeof(kvpairsKey)),
                         kvpairs.getRange(0, kvpairs.size()),
                         kvpairs.size(), &rejectRules);
         } catch (RejectRulesException& e) {
            // Should we have retrys on dangling adds?
            // Probably not... Technically no one should try to add the same
            // assoc edge twice. And a fsck process should clean up dangling
            // edges.
            throw;
         }

        while (true) {
            Buffer value{};
            uint64_t readVersion = 0;

            AssocListKey akey{id1, atype};
            try {
                client->read(assocTableId,
                             &akey, downCast<uint16_t>(sizeof(akey)),
                             &value,
                             nullptr, &readVersion);
            } catch (RejectRulesException& e) {
                assert(value.size() == 0);
                // Empty buffer is implicity an empty AssocList.
                return;
            }

            AssocList list{&value};
            list.add(id2, time);

            try {
                RejectRules rejectRules{0, 0, 0, 0, 0};
                if (readVersion == 0) {
                    rejectRules.givenVersion = readVersion;
                    rejectRules.versionLeGiven = 1;
                } else {
                    rejectRules.doesntExist = 1;
                }
                client->write(assocTableId,
                              &akey, downCast<uint16_t>(sizeof(akey)),
                              value.getRange(0, value.size()), value.size(),
                              &rejectRules);
            } catch (RejectRulesException& e) {
                // Something changed in the assoc list so start over.
                continue;
            }
        }
    }

    /// assoc_delete(id1, atype, id2): deletes the given association
    void assocDelete(Id id1, AType atype, Id id2) {
        while (true) {
            Buffer value{};
            uint64_t readVersion = 0;

            AssocListKey akey{id1, atype};
            try {
                RejectRules rejectRules{0, 0, 0, 0, 0};
                rejectRules.exists = 1;
                client->read(assocTableId,
                             &akey, downCast<uint16_t>(sizeof(akey)),
                             &value,
                             &rejectRules, &readVersion);
            } catch (RejectRulesException& e) {
                // No assoc list so nothing to delete.
                return;
            }

            assert(value.size() > 0);
            AssocList list{&value};
            list.remove(id2);

            try {
                RejectRules rejectRules{0, 0, 0, 0, 0};
                rejectRules.givenVersion = readVersion;
                rejectRules.versionLeGiven = 1;
                client->write(assocTableId,
                              &akey, downCast<uint16_t>(sizeof(akey)),
                              value.getRange(0, value.size()), value.size(),
                              &rejectRules);
            } catch (RejectRulesException& e) {
                // Something changed in the assoc list so start over.
                continue;
            }
        }

        // No need to add RejectRules here. TAO guarantees id monotonicity, so
        // this 3-tuple will never appear in the table again, so no concern
        // about ABA mistargeting the remove somehow.
        AssocKey kvpairsKey{id1, atype, id2};
        client->remove(assocTableId,
                       &kvpairsKey, downCast<uint16_t>(sizeof(kvpairsKey)));
    }

#if 0
  /**
   * assoc_change_type(id1, atype, id2, newtype): changes the association
   * (id1, atype, id2) to (id1, newtype, id2).
   */
  void assocChangeType(uint64_t id1, uint16_t atype, uint64_t id2, uint16_t newtype) {
      return;
  }
#endif

    /**
     * assoc_get(id1, atype, id2set): returns all of the associations (id1,
     * atype, id2) and their time and data, where id2 in id2set.
     */
    void assocGet(Id id1, AType atype, Id* id2set, uint32_t nId2set,
                  Buffer& kvpairs)
    {
        Buffer value{};
        uint64_t readVersion = 0;

        AssocListKey akey{id1, atype};
        try {
            RejectRules rejectRules{0, 0, 0, 0, 0};
            rejectRules.doesntExist = 1;
            client->read(assocTableId,
                         &akey, downCast<uint16_t>(sizeof(akey)),
                         &value,
                         &rejectRules, &readVersion);
        } catch (RejectRulesException& e) {
            // No assoc list so nothing to get.
            return;
        }

        assert(value.size() > 0);
        AssocList list{&value};
        list.filter(id2set, &nId2set);

        MultiReadObject requestObjects[nId2set];
        MultiReadObject* requests[nId2set];
        Tub<ObjectBuffer> values[nId2set]; // XXX Pry pass in?
        AssocKey keys[nId2set];

        for (uint32_t i = 0; i < nId2set; ++i) {
            requestObjects[i] =
                MultiReadObject(assocTableId,
                                &keys[i],
                                downCast<uint16_t>(sizeof(keys[i])),
                                &values[i]);
            requests[i] = &requestObjects[i];
        }
        client->multiRead(requests, nId2set);
    }

#if 0
  /**
   * assoc_count(id1, atype): returns the size of the association list for
   * (id1, atype), which is the number of edges of type atype that originate
   * at id1.
   */
  uint32_t assocCount(uint64_t id1, uint16_t atype) {
      return {};
  }

  /**
   * assoc_range(id1, atype, pos, limit): returns elements of the (id1,
   * atype) association list with index i in [pos,pos+limit].
   */
  KVSet assocRange(uint64_t id1, uint16_t atype, uint32_t pos, uint16_t limit) {
      return {};
  }

  /**
   * assoc_time_range(id1, atype, high, low, limit): returns elements from
   * the (id1, atype) association list, ordered by time.
   */
  KVSet assocTimeRange(uint64_t id1, uint16_t atype, uint32_t high, uint32_t low, uint16_t limit) {
      return {};
  }
#endif

  private:
    /**
     * Create needed tables on RAMCloud for TAO.
     * Idempotent.
     */
    void initialize() {
        objectTableId = client->createTable("objects");
        assocTableId = client->createTable("assocs");
        uint64_t value = 0;
        client->write(objectTableId,
                      &nextIdKey, downCast<uint16_t>(sizeof(nextIdKey)),
                      &value, sizeof(value));
    }

    RamCloud* client;
    uint64_t objectTableId;
    uint64_t assocTableId;

    Id nextId;
};

class TAOTest
{
  public:
    TAOTest(RamCloud* client)
        : tao{client}
    {}

    void runAll() {
        test_objectAdd();
        printf("--- Tests complete ---\n");
    }

    void test_objectAdd() {
        Buffer kvpairs{};

        TAO::Id id1 = tao.objectAdd(0x0b, kvpairs);
        assert(id1 == 1);

        TAO::Id id2 = tao.objectAdd(0x0b, kvpairs);
        assert(id2 == 2);

        tao.assocAdd(1, 0x45, 2, 10, kvpairs);
    }

  private:
    TAO tao;
};

int
main(int argc, char *argv[])
try
{
    int clientIndex;
    int numClients;

    // Set line buffering for stdout so that printf's and log messages
    // interleave properly.
    setvbuf(stdout, NULL, _IOLBF, 1024);

    OptionsDescription clientOptions("Client");
    clientOptions.add_options()
      // These first two options are currently ignored. They're here so that
      // this script can be run with cluster.py.
      ("clientIndex",
       ProgramOptions::value<int>(&clientIndex)->
          default_value(0),
       "Index of this client (first client is 0; currently ignored)")
      ("numClients",
       ProgramOptions::value<int>(&numClients)->
          default_value(1),
       "Total number of clients running (currently ignored)");

    OptionParser optionParser(clientOptions, argc, argv);
    RamCloud client(&optionParser.options);

    TAOTest t{&client};
    t.runAll();

    return 0;
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
