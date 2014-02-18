external_sort
-------------

This is a header-only, multithreaded, policy-based implementation of the [external sort](http://en.wikipedia.org/wiki/External_sorting) in C++11.

The library works with fundamental data types as well as with user defined custom data types.

### External sort algorithm

#### Phase 1: split and sort

A big input file is consequently read in pieces (aka blocks or chunks) small enough to fit into the memory. Each piece is sorted and written to a separate output file (split).

There is one thread reading data from the input file. For each block, read but not yet sorted, a new worker thread is spawned to sort it and write the block to the output file.

Example:

    external_sort::SplitParams params;
    params.mem.size   = 100;                // memory size
    params.mem.unit   = external_sort::MB;  // memory unit
    params.mem.blocks = 2;                  // max number of memory blocks
    params.spl.ifile  = "input_file";       // input file to split/sort
    params.spl.ofile  = "output_file";      // output file prefix
    
    external_sort::split<ValueType>(params);
    if (params.err) {
        LOG_ERR(("Error: %s") % params.err.msg());
    }

#### Phase 2: merge

The input files (sorted splits) are merged repeatedly until only one file left.

There can be more than one ongoing merge at a time. Each merge takes k input files (streams) and merges them into one output file (stream). Each input/output stream has its own thread reading/writing data asynchronously. Hence, each k-merge consists of: k threads reading the data (k input streams), 1 thread performing the actual merge and 1 thread writing the data (the output stream).

Each stream (input or output) has a queue and at least two blocks of data. Two blocks per stream make it possible to perform read/write and merge in two threads in parallel (each thread has its own block to work with). Reasonably, there shall be no need in more than two blocks, since either reading/writing or merging is supposed to be consistently slower than the other.

Example:

    external_sort::MergeParams params;
    params.mem.size      = 100;                // memory size
    params.mem.unit      = external_sort::MB;  // memory unit
    params.mrg.merges    = 4;                  // number of simultaneous merges
    params.mrg.kmerge    = 4;                  // number of streams to merge
    params.mrg.stmblocks = 2;                  // number of memory blocks per i/o stream
    params.mrg.ifiles    = files;              // std::list of input files
    params.mrg.ofile     = "file_merged";      // output file

    external_sort::merge<ValueType>(params);
    if (params.err) {
        LOG_ERR(("Error: %s") % params.err.msg());
    }

### External sort = split + merge

It is possible to combine both split and merge into a single function call:

    // set split and merge parameters
    external_sort::SplitParams sp;
    external_sort::MergeParams mp;
    sp.mem.size = 10;
    sp.mem.unit = external_sort::MB;
    mp.mem = sp.mem;
    sp.spl.ifile = "big_input_file";
    mp.mrg.ofile = "big_sorted_file";

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

### The tool

In the ./example sub-directory, there is a simple wrapper tool around the external sort functionality of the library.
By default, it sorts uint32_t values (it can be changed to a custom type, see [external_sort_custom.hpp](https://github.com/alveko/external_sort/blob/master/example/external_sort_custom.hpp)).

    Usage: external_sort [options]
    
    General options:
      -h [ --help ]                         Display this information
                                            
      --act arg (=all)                      Action to perform. Possible values:
                                            <gen | spl | mrg | chk | all | srt>
                                            gen - Generates random data
                                            spl - Splits and sorts the input
                                            mrg - Merges the input
                                            chk - Checks if the input is sorted
                                            all - All of the above
                                            srt = spl + mrg
                                            
      --msize arg (=1)                      Memory size
      --munit arg (=M)                      Memory unit: <B | K | M>
      --log arg (=4)                        Log level: [0-6]
      --no_rm                               Do not remove temporary files
      --tmpdir arg (=<same as i/o files>)   Directory for temporary files
                                            (relevant if act includes mrg)
    
    Options for act=gen (generate):
      --gen.ofile arg (=generated)          Output file
      --gen.fsize arg                       File size to generate, in memory units.
                                            By default: gen.fsize = 16 * msize
      --gen.blocks arg (=2)                 Number of blocks in memory
    
    Options for act=spl (phase 1: split and sort):
      --srt.ifile arg                       Same as --spl.ifile
      --spl.ifile arg (=<gen.ofile>)        Input file
      --spl.ofile arg (=<spl.ifile>)        Output file prefix
      --spl.blocks arg (=2)                 Number of blocks in memory
    
    Options for act=mrg (phase 2: merge):
      --mrg.ifiles arg (=<sorted splits>)   Input files to be merged into one
                                            (required and only relevant if act=mrg,
                                            otherwise the list of files, i.e. 
                                            sorted splits, is passed over from 
                                            phase 1)
      --mrg.ofile arg (=<spl.ifile>.sorted) Output file (required if act=mrg)
      --mrg.merges arg (=4)                 Number of simultaneous merge merges
      --mrg.kmerge arg (=4)                 Number of streams merged at a time
      --mrg.stmblocks arg (=2)              Number of memory blocks per stream
    
    Options for act=chk (check):
      --chk.ifile arg (=<mrg.ofile>)        Input file
      --chk.blocks arg (=2)                 Number of blocks in memory
