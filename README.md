external_sort
-------------

External sort implementation in C++11.

    Usage: external_sort [options]

    General options:
      -h [ --help ]                         Display this information

      --act arg (=all)                      Action to perform. Possible values:
                                            <gen | srt | mrg | chk | all | ext>
                                            gen - Generates random data
                                            srt - Splits and sorts the input
                                            mrg - Merges the input
                                            chk - Checks if the input is sorted
                                            all - All of the above
                                            ext = srt + mrg

      --msize arg (=1)                      Memory size
      --munit arg (=M)                      Memory unit: <B | K | M>
      --log arg (=4)                        Log level: [0-6]
      --no_rm                               Do not remove temporary files
      --tmpdir arg (=<same as input files>) Directory for temporary files

    Options for act=gen (generate):
      --gen.ofile arg (=generated)          Output file
      --gen.fsize arg                       File size to generate, in memory units.
                                            By default: gen.fsize = 16 * msize
      --gen.blocks arg (=2)                 Number of blocks in memory

    Options for act=srt (phase 1: split and sort):
      --srt.ifile arg (=<gen.ofile>)        Input file
      --srt.blocks arg (=2)                 Number of blocks in memory

    Options for act=mrg (phase 2: merge):
      --mrg.ifiles arg (=<sorted splits>)   Input files to be merged into one
                                            (required and only relevant if act=mrg,
                                            otherwise the list of files, i.e.
                                            sorted splits, is passed over from
                                            phase 1)
      --mrg.ofile arg (=<srt.ifile>.sorted) Output file (required if act=mrg)
      --mrg.tasks arg (=4)                  Number of simultaneous merge tasks
      --mrg.nmerge arg (=4)                 Number of streams merged at a time
      --mrg.blocks arg (=2)                 Number of memory blocks per stream

    Options for act=chk (check):
      --chk.ifile arg (=<mrg.ofile>)        Input file
      --chk.blocks arg (=2)                 Number of blocks in memory
