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

#include <fstream>
using std::endl;
using std::ofstream;

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
        // if (fd < 0)
        //     throw FatalError(HERE, "Couldn't open /dev/urandom", errno);
        unsigned int seed;
        ssize_t bytesRead = read(fd, &seed, sizeof(seed));
        close(fd);
        assert(bytesRead == sizeof(seed));
        statebuf = static_cast<char*>(malloc(STATE_BYTES));
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
        double u = static_cast<double>(_generateRandom()) /
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


int compareInt(const void * a, const void * b)
{
  return (*(int*)b - *(int*)a);
}

int main(int argc, char const *argv[])
{
	int numKeys = 2000, i, count = 100000000;

	ZipfianGenerator generator(numKeys);

    int *histogram = static_cast<int *>(malloc(numKeys * sizeof(histogram[0])));
    memset(histogram, 0, numKeys * sizeof(histogram[0]));

	for (i = 0; i < count; ++i) {
		histogram[generator.nextNumber()]++;
	}
	qsort(histogram, numKeys, sizeof(histogram[0]), compareInt);

	ofstream zipf_data;
	zipf_data.open("zipf.data");
	for (i = 0; i < 50; ++i) {
		zipf_data << histogram[i] << endl;
	}
	zipf_data.close();

	free(histogram);
	return 0;
}