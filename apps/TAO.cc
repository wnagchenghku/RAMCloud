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
#include "Util.h"

using namespace RAMCloud;

// TODO
// - Need something like htonl on ids.

#define log(F, ...) fprintf(stderr, F, __VA_ARGS__);
//#define log noLog

void noLog(const char* format, ...) {}

void hexDumpBuffer(Buffer* buffer) {
    uint32_t l = buffer->size();
    fprintf(stderr, "%s\n",
            Util::hexDump(buffer->getRange(0, l), l).c_str());
}

size_t constexpr static_strlen(const char* str)
{
    return *str ? 1 + static_strlen(str + 1) : 0;
}


static constexpr char const* nextIdKey = "__nextId";

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
      public: // PRIVATE
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
            return *buffer->getOffset<Assoc>(i * uint32_t(sizeof(Assoc)));
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

        void dump() {
            fprintf(stderr, "AssocList with %u elements\n", size());
            for (uint32_t i = 0; i < size(); ++i) {
                fprintf(stderr, "  Id %lu Time %u\n", at(i).id, at(i).time);
            }
        }

        /**
         * \param id2s
         *      A set of Ids to test for in this list. Ids are rearranged
         *      within this array, and the value at \a nId2s is adjusted
         *      to describe which prefix of the values in the array are
         *      represented in this list.
         */
        void filter(Id* id2s, uint32_t* nId2s) {
            for (uint32_t j = 0; j < *nId2s; ++j) {
                bool match = false;
                for (uint32_t i = 0; i < size(); ++i) {
                    log("Comparing %lu and %lu ids\n", at(i).id, id2s[j]);
                    if (id2s[j] == at(i).id) {
                        match = true;
                        break;
                    }
                }
                if (!match) {
                    std::swap(id2s[j], id2s[*nId2s - 1]);
                    --(*nId2s);
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
        log("objectAdd id == %lu\n", id);
        log("objectAdd kvpairs.size() == %u\n", kvpairs.size());
        kvpairs.emplacePrepend<ObjectHeader>(otype);
        log("objectAdd kvpairs.size() == %u\n", kvpairs.size());
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
        log("objectGet kvpairs.size() == %u\n", kvpairs.size());
        ObjectHeader* header = kvpairs.getStart<ObjectHeader>();
        kvpairs.truncateFront(sizeof(*header));
        log("objectGet kvpairs.size() == %u\n", kvpairs.size());
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
           log("write assoc %lu - %u - %lu\n", id1, atype, id2);
           client->write(assocTableId,
                         &kvpairsKey, downCast<uint16_t>(sizeof(kvpairsKey)),
                         kvpairs.getRange(0, kvpairs.size()),
                         kvpairs.size(), &rejectRules);
         } catch (RejectRulesException& e) {
            // Should we have retrys on dangling adds?
            // Probably not... Technically no one should try to add the same
            // assoc edge twice. And a fsck process should clean up dangling
            // edges from clients that have died.
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
                log("write assoc %lu - %u - *\n", id1, atype);
                client->write(assocTableId,
                              &akey, downCast<uint16_t>(sizeof(akey)),
                              value.getRange(0, value.size()), value.size(),
                              &rejectRules);
                break;
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
     *
     * \a values must be an array with at least \a nId2set entries. On
     * return \a nId2set will indicate the number of valid values in
     * \a values.
     */
    void assocGet(Id id1, AType atype, Id* id2set, uint32_t* nId2set,
                  Tub<ObjectBuffer>* kvpairs)
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
            *nId2set = 0;
            return;
        }

        assert(value.size() > 0);
        AssocList list{&value};
        list.filter(id2set, nId2set);

        MultiReadObject requestObjects[*nId2set];
        MultiReadObject* requests[*nId2set];
        AssocKey keys[*nId2set];

        for (uint32_t i = 0; i < *nId2set; ++i) {
            keys[i] = { id1, atype, id2set[i] };
            requestObjects[i] =
                MultiReadObject(assocTableId,
                                &keys[i],
                                downCast<uint16_t>(sizeof(keys[i])),
                                &kvpairs[i]);
            requests[i] = &requestObjects[i];
        }
        client->multiRead(requests, *nId2set);
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

    void reset() {
        client->dropTable("objects");
        client->dropTable("assocs");
        initialize();
    }

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

class TAOTest {
  public:
    TAOTest(RamCloud* client)
        : tao{client}
    {}

    ~TAOTest()
    {
        tao.reset();
    }

    static void runAll(RamCloud* client) {
        void (TAOTest::* tests[])(void) = {
            &TAOTest::test_filter,
            &TAOTest::test_objectAdd,
            &TAOTest::test_assocAdd,
        };
        for (auto test : tests) {
            printf("- Test start -\n");
            TAOTest t{client};
            (t.*test)();
            printf("- Test end -\n");
        }
        printf("--- Tests complete ---\n");
    }

    void test_filter() {
        TAO::Id ids[4] = { 0, 1, 2, 3 };
        TAO::Time t = 0;

        Buffer value{};
        TAO::AssocList l{&value};
        l.add(ids[1], t);
        l.dump();
        assert(value.size() == sizeof(TAO::AssocList::Assoc));
        assert(l.size() == 1);

        uint32_t nId2set = 1;
        TAO::Id id2set[nId2set] = { 2 };
        l.filter(id2set, &nId2set);
        assert(nId2set == 0);

        nId2set = 1;
        id2set[0] = { 1 };
        l.filter(id2set, &nId2set);
        assert(nId2set == 1);
        assert(id2set[0] == 1);

        l.add(ids[3], t); // list contains 1 and 3 now.
        assert(value.size() == 2 * sizeof(TAO::AssocList::Assoc));
        assert(l.size() == 2);
        l.dump();
        {
            uint32_t nId2set = 2;
            TAO::Id id2set[nId2set] = { 2, 3 };
            l.filter(id2set, &nId2set);
            assert(nId2set == 1);
            assert(id2set[0] == 3);
        }

        {
            uint32_t nId2set = 2;
            TAO::Id id2set[nId2set] = { 1, 3 };
            l.filter(id2set, &nId2set);
            assert(nId2set == 2);
            assert(id2set[0] == 1);
            assert(id2set[1] == 3);
        }

        {
            uint32_t nId2set = 3;
            TAO::Id id2set[nId2set] = { 1, 2, 3 };
            l.filter(id2set, &nId2set);
            assert(nId2set == 2);
            assert(id2set[0] == 1);
            assert(id2set[1] == 3);
        }
    }

    void test_objectAdd() {
        Buffer kvpairs{};
        const char* test = "test";
        kvpairs.append(test, uint32_t(strlen(test)));

        TAO::Id id1 = tao.objectAdd(0x0b, kvpairs);
        assert(id1 == 1);

        kvpairs.append("2", 1);
        TAO::Id id2 = tao.objectAdd(0x0b, kvpairs);
        assert(id2 == 2);

        kvpairs.reset();
        TAO::OType otype = tao.objectGet(id1, kvpairs);
        assert(otype == 0x0b);
        log("kvpairs.size() == %u\n", kvpairs.size());
        assert(kvpairs.size() == 4);
        assert(strcmp(static_cast<char*>(kvpairs.getRange(0, 4)), "test") == 0);

        otype = tao.objectGet(id2, kvpairs);
        assert(otype == 0x0b);
        assert(strcmp(static_cast<char*>(kvpairs.getRange(0, 5)), "test2"));
    }

    void test_assocAdd() {
        Buffer kvpairs{};

        std::vector<TAO::Id> ids{};
        for (uint32_t i = 1; i <= 3; ++i) {
            kvpairs.reset();
            char c = static_cast<char>('0' + i);
            kvpairs.append(&c, 1);
            TAO::Id id = tao.objectAdd(0x0b, kvpairs);
            assert(id == i);
            ids.push_back(id);
        }

        // 1->2
        kvpairs.reset();
        kvpairs.append("1->2", 4);
        tao.assocAdd(1, 0x45, 2, 10, kvpairs);

        { // 1->2 is there.
            uint32_t nId2set = 1;
            TAO::Id id2set[nId2set] = { 2 };
            Tub<ObjectBuffer> values[nId2set]{};
            tao.assocGet(1, 0x45, id2set, &nId2set, values);
            assert(nId2set == 1);
            assert(bool(values[0]) == true);
            assert(memcmp(values[0]->getValue(), "1->2", 4) == 0);
        }

        { // 1->1 not there.
            uint32_t nId2set = 1;
            TAO::Id id2set[nId2set] = { 1 };
            Tub<ObjectBuffer> values[nId2set]{};
            tao.assocGet(2, 0x45, id2set, &nId2set, values);
            assert(bool(values[0]) == false);
        }

        { // 2->1 not there.
            uint32_t nId2set = 1;
            TAO::Id id2set[nId2set] = { 1 };
            Tub<ObjectBuffer> values[nId2set]{};
            tao.assocGet(2, 0x45, id2set, &nId2set, values);
            assert(nId2set == 0);
        }

        { // 2->2 not there.
            uint32_t nId2set = 1;
            TAO::Id id2set[nId2set] = { 2 };
            Tub<ObjectBuffer> values[nId2set]{};
            tao.assocGet(2, 0x45, id2set, &nId2set, values);
            assert(nId2set == 0);
        }

        // 2 -> 1
        kvpairs.reset();
        kvpairs.append("2->1", 4);
        tao.assocAdd(2, 0x45, 1, 10, kvpairs);

        { // 2->1 is there.
            uint32_t nId2set = 1;
            TAO::Id id2set[nId2set] = { 1 };
            Tub<ObjectBuffer> values[nId2set]{};
            tao.assocGet(2, 0x45, id2set, &nId2set, values);
            assert(nId2set == 1);
            assert(bool(values[0]) == true);
            assert(memcmp(values[0]->getValue(), "2->1", 4) == 0);
        }

        { // 2->2 is not there.
            uint32_t nId2set = 1;
            TAO::Id id2set[nId2set] = { 2 };
            Tub<ObjectBuffer> values[nId2set]{};
            tao.assocGet(2, 0x45, id2set, &nId2set, values);
            assert(bool(values[0]) == false);
        }

        kvpairs.reset();
        kvpairs.append("2->3", 4);
        tao.assocAdd(2, 0x45, 3, 10, kvpairs);

        { // 2->{1, 3} are there.
            uint32_t nId2set = 3;
            TAO::Id id2set[nId2set] = { 1, 2, 3 };
            Tub<ObjectBuffer> values[nId2set]{};
            tao.assocGet(2, 0x45, id2set, &nId2set, values);
            assert(nId2set == 2);
            assert(bool(values[0]) == true);
            assert(bool(values[1]) == true);
            assert(memcmp(values[0]->getValue(), "2->1", 4) == 0);
            assert(memcmp(values[1]->getValue(), "2->3", 4) == 0);
        }

        { // Ensure nothing on other assocs.
            uint32_t nId2set = 3;
            TAO::Id id2set[nId2set] = { 1, 2, 3 };
            Tub<ObjectBuffer> values[nId2set]{};
            tao.assocGet(2, 0x46, id2set, &nId2set, values);
            assert(nId2set == 0);
        }
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

    TAOTest::runAll(&client);

    return 0;
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
