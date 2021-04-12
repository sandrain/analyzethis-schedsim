### AnalyzeThis scheduling simulator

AnalyzeThis is an analysis workflow-aware storage system. More information can be found in
the following papers:

* Hyogi Sim, Geoffroy Vallee, Youngjae Kim, Sudharshan S. Vazhkudai, Devesh
Tiwari, and Ali R. Butt, ["An Analysis Workflow-Aware Storage System for
Multi-Core Active Flash Arrays,"](https://doi.org/10.1109/TPDS.2018.2865471) IEEE Transactions on Parallel Distributed
Systems (TPDS), vol. 30, no. 2, pp. 271–285, Feb. 2019.

* Hyogi Sim, Youngjae Kim, Sudharshan S. Vazhkudai, Devesh Tiwari, Ali Anwar,
Ali R. Butt, and Lavanya Ramakrishnan, ["AnalyzeThis: An Analysis Workflow-Aware
Storage System,"](https://doi.org/10.1145/2807591.2807622) in Proceedings of the International Conference for High
Performance Computing, Networking, Storage and Analysis (SC), New York, NY,
USA, 2015

This simulator is to study the performance under large-scale and multi-core environments,
which is difficult from the [emulation framework](https://github.com/sandrain/anfs).

### How to run

Run with master branch. dev branch might not be stable.

* [master] Tested on python 2.7.5
* [dev] will be unstable.

BharathiPaper directory contains the snapshot of the workflow generator, which
was used in the [paper by Bharathi (Characterization of Scientific Workflows)](https://confluence.pegasus.isi.edu/download/attachments/2490624/Workflow-generator-works08.pdf?version=1&modificationDate=1254808345000&api=v2).

All resources regarding the paper could be found [here](https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator).

### Single Host Simulation

The simulator can be used to simulate a single host with n AFEs and N cores. To activate
this mode, simply do *not* specify any hosts ('-N' argument). For instance,

```
./sim.py -c 2 -n 4 workflows/montage_60.xml
```

simulates a single host with 2 cores and 4 AFEs.

This simulation mode relies on the scheduler from the [libanalyzethis](https://github.com/sandrain/libanalyzethis)
library. To enable it,
you must specify where the library is installed using the `PYTHONPATH` environment variable.

```
export PYTHONPATH=/where/libanalyzethis/is/src:$PYTHONPATH
```

### Multi-host Simulation

The simulator can be used to simulate a distributed platform. In this context, the simulated
distributed platform can include several servers (nodes hosting the AFEs) and several
clients. This mode requires the [libanalyzethis](https://github.com/sandrain/libanalyzethis)
library and it is required to specify where
the library is installed using the `PYTHONPATH` environment variable.

```
export PYTHONPATH=/where/libanalyzethis/is/src:$PYTHONPATH
```

To activate the multi-host mode, simply specify the number of nodes on the command line:

```
./sim.py -N 2 -c 2 -n 2 workflows/montage_60.xml
```

This will simulates 2 server nodes, each node having 2 cores and 2 AFEs.
