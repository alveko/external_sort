#include <string>
#include <vector>
#include <queue>
#include <iterator>
#include <algorithm>

#include <boost/program_options.hpp>

#include <boost/format.hpp>
#include <boost/timer/timer.hpp>
#include <boost/utility.hpp>

#include "external_sort_aux.h"
#include "data_stream.hpp"
#include "data_stream_merge.hpp"

namespace po = boost::program_options;

// functor is inlined and is faster than a callback function
template <typename T>
struct generate_data
{
    T operator()()
    {
        union {
            T data;
            uint8_t bytes[sizeof(T)];
        } u;

        for (auto& b : u.bytes) {
            b = rand() & 0xFF;
        }
        TDEBUG(("data  = %s\n") % u.data);
        return u.data;
    }
};

template <typename T, typename Generator = generate_data<T> >
void generate_vector(std::vector<T>& v, size_t n, Generator gen = Generator())
{
    TRACE(("\nGenerating data n = %d...\n") % n);
    TIMER("Done in %t sec CPU, %w sec real\n");

    std::generate(v.begin(), v.end(), gen);

    TRACE(("min = %s\nmax = %s\n")
          % *std::min_element(v.begin(), v.end())
          % *std::max_element(v.begin(), v.end()));
}

int main(int argc, char *argv[])
{
    srand(time(NULL));

    std::string sfilename("external_data");
    //size_t msize = 10;
    //size_t fsize = 100;

    size_t ssize = 10;
    size_t nstreams = 2;
    size_t nmerge = 2;

    po::options_description usage("Allowed options");
    usage.add_options()
        ("help,h", "Show help")
        ("action,a", po::value<std::string>()->required(),
         "Action to perform: <generate | sort>")
            /*
        ("file", po::value<std::string>(&sfilename)->default_value(sfilename),
         "File with data (either input or output)")
        ("msize,m", po::value<size_t>(&msize)->default_value(msize),
         "Memory size (Mb)")
        ("fsize", po::value<size_t>(&fsize)->default_value(fsize),
         "File size (Mb)")
            */
        ("ssize", po::value<size_t>(&ssize)->default_value(ssize),
         "Input stream size")
        ("nstreams", po::value<size_t>(&nstreams)->default_value(nstreams),
         "Number of input streams")
        ("nmerge", po::value<size_t>(&nmerge)->default_value(nmerge),
         "Number of streams merged at a time")
            ;

    // parse command line arguments
    po::variables_map vm;
    try {
        po::store(po::parse_command_line(argc, argv, usage), vm);
        if (vm.count("help")) {
            std::cout << usage << std::endl;
            return 1;
        }
        po::notify(vm);
    } catch (std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        std::cout << usage << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "Unknown error!" << "\n";
        return false;
    }

    TDEBUG(("DEBUG ON\n"));
    TRACE(("Input stream size       : %s\n") % ssize);
    TRACE(("Number of input streams : %s\n") % nstreams);
    TRACE(("Nb of streams merged    : %s\n") % nmerge);

    // generate input streams
    using U32Stream = DataStream<uint32_t, (uint32_t)-1, std::less<uint32_t>>;
    DataStreamsQueue<U32Stream> streams;

    TRACE(("ssize = %s\n") % ssize);
    while (streams.size() < nstreams) {

        DataStreamPtr<U32Stream> ps(new U32Stream(ssize));

        ps->data().resize(ssize);
        generate_vector(ps->data(), ssize);

        std::sort(ps->data().begin(), ps->data().end());

        streams.push(ps);
    }

    // merge all streams into one
    merge_all(streams, nmerge);

    return 0;
}
