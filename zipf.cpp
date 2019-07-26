/* g++ zipf.cpp -std=c++11 */
#include <stdlib.h>
#include <algorithm>
#include <iostream>
#include <math.h>
#include <cinttypes>

#include <ctype.h>
#include <cxxabi.h>
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <assert.h>
#include <unistd.h>
#include <cstring>
#include <time.h>
#include <sys/time.h>
#include <fstream>
using std::endl;
using std::ofstream;

namespace Memory {
void*
xmalloc(size_t len)
{
    void *p = malloc(len > 0 ? len : 1);
    if (p == NULL) {
        fprintf(stderr, "malloc(%lu) failed\n", len);
        exit(1);
    }

    return p;
}
} // namespace Memory

uint64_t
_generateRandom()
{
    // Internal scratch state used by random_r 128 is the same size as
    // initstate() uses for regular random(), see manpages for details.
    // statebuf is malloc'ed and this memory is leaked, it could be a __thread
    // buffer, but after running into linker issues with large thread local
    // storage buffers, we thought better.
    enum { STATE_BYTES = 128 };
    static __thread char* statebuf;
    // random_r's state, must be handed to each call, and seems to refer to
    // statebuf in some undocumented way.
    static __thread random_data buf;

    if (statebuf == NULL) {
        int fd = open("/dev/urandom", O_RDONLY);
        if (fd < 0) {
             fprintf(stderr, "Couldn't open /dev/urandom");
             exit(1);
        }
        unsigned int seed;
        ssize_t bytesRead = read(fd, &seed, sizeof(seed));
        close(fd);
        assert(bytesRead == sizeof(seed));
        statebuf = static_cast<char*>(Memory::xmalloc(STATE_BYTES));
        initstate_r(seed, statebuf, STATE_BYTES, &buf);
    }

    // Each call to random returns 31 bits of randomness,
    // so we need three to get 64 bits of randomness.
    static_assert(RAND_MAX >= (1 << 31), "RAND_MAX too small");
    int32_t lo, mid, hi;
    random_r(&buf, &lo);
    random_r(&buf, &mid);
    random_r(&buf, &hi);
    uint64_t r = (((uint64_t(hi) & 0x7FFFFFFF) << 33) | // NOLINT
                  ((uint64_t(mid) & 0x7FFFFFFF) << 2)  | // NOLINT
                  (uint64_t(lo) & 0x00000003)); // NOLINT
    return r;
}

static inline uint64_t
generateRandom()
{
    return _generateRandom();
}

class ZipfianGenerator {
  public:
    /**
     * Construct a generator.  This may be expensive if n is large.
     *
     * \param n
     *      The generator will output random numbers between 0 and n-1.
     * \param theta
     *      The zipfian parameter where 0 < theta < 1 defines the skew; the
     *      smaller the value the more skewed the distribution will be. Default
     *      value of 0.99 comes from the YCSB default value.
     */
    explicit ZipfianGenerator(uint64_t n, double theta = 0.99)
        : n(n)
        , theta(theta)
        , alpha(1 / (1 - theta))
        , zetan(zeta(n, theta))
        , eta((1 - pow(2.0 / static_cast<double>(n), 1 - theta)) /
              (1 - zeta(2, theta) / zetan))
    {}

    /**
     * Return the zipfian distributed random number between 0 and n-1.
     */
    uint64_t nextNumber()
    {
        double u = static_cast<double>(generateRandom()) /
                   static_cast<double>(~0UL);
        double uz = u * zetan;
        if (uz < 1)
            return 0;
        if (uz < 1 + std::pow(0.5, theta))
            return 1;
        return 0 + static_cast<uint64_t>(static_cast<double>(n) *
                                         std::pow(eta*u - eta + 1.0, alpha));
    }

  private:
    const uint64_t n;       // Range of numbers to be generated.
    const double theta;     // Parameter of the zipfian distribution.
    const double alpha;     // Special intermediate result used for generation.
    const double zetan;     // Special intermediate result used for generation.
    const double eta;       // Special intermediate result used for generation.

    /**
     * Returns the nth harmonic number with parameter theta; e.g. H_{n,theta}.
     */
    static double zeta(uint64_t n, double theta)
    {
        double sum = 0;
        for (uint64_t i = 0; i < n; i++) {
            sum = sum + 1.0/(std::pow(i+1, theta));
        }
        return sum;
    }
};


int compare(const void * a, const void * b)
{
    // return (*(int*)a - *(int*)b); // ascending order
    return (*(uint64_t*)b - *(uint64_t*)a); // descending order
}

int main(int argc, char const *argv[])
{
    int numKeys = 300000000, i, seconds = 20;
    double zipfSkew = 0.99;

    ZipfianGenerator generator(numKeys, zipfSkew);

    uint64_t *histogram = static_cast<uint64_t *>(Memory::xmalloc(numKeys * sizeof(histogram[0])));
    memset(histogram, 0, numKeys * sizeof(histogram[0]));

    struct timeval start, end;
    uint64_t opCount = 0;
    gettimeofday(&start, NULL);
    do {
      histogram[generator.nextNumber()]++;
      gettimeofday(&end, NULL);
      opCount++;
    } while ((end.tv_sec - start.tv_sec) < seconds);

    qsort(histogram, numKeys, sizeof(histogram[0]), compare);

    //ofstream zipf_data;
    //zipf_data.open("zipf.dat");
    //for (i = 0; i < 10000; ++i) {
    //  zipf_data << i << " " << ((double)histogram[i] / (double)opCount) << endl;
    //}
    //zipf_data.close();

    uint64_t sum = 0;
    int percent = 20;
    for (i = 0; i < numKeys * percent / 100; ++i) {
      sum += histogram[i];
    }

    std::cout << "top " << percent << "% occupies " <<  (double)sum / (double)opCount << " accesses." << std::endl;

    free(histogram);
    return 0;
}