// can be compiled with:
// g++ -std=c++11 -I.. -pthread simple_example1.cc -o ./simple1

#include <iostream>

#include "external_sort.hpp"

int main()
{
    // set split and merge parameters
    external_sort::SplitParams sp;
    external_sort::MergeParams mp;
    sp.mem.size = 10;
    sp.mem.unit = external_sort::MB;
    mp.mem = sp.mem;
    sp.spl.ifile = "/dir1/big_input_file";
    mp.mrg.ofile = "/dir2/big_sorted_file";

    using ValueType = unsigned int;

    // run external sort
    external_sort::sort<ValueType>(sp, mp);

    if (sp.err.none && mp.err.none) {
        std::cout << "File sorted successfully!" << std::endl;
    } else {
        std::cout << "External sort failed!" << std::endl;
        if (sp.err) {
            std::cout << "Split failed: " << sp.err.msg() << std::endl;
        } else {
            std::cout << "Merge failed: " << mp.err.msg() << std::endl;
        }
    }
}
