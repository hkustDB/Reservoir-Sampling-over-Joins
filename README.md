# Reservoir Sampling over Joins
This project provides the experiment code and scripts of our paper: Reservoir Sampling over Joins.

## Structure
* RSJoin: Code and scripts of our algorithms `RSJoin` and `RSJoin_opt`.
* SJoinMod: Modifications to the baseline [SJoin](https://github.com/InitialDLab/SJoin) in order to support more experiments.

## Queries
The queries used in our experiments are included in [Queries](Queries.md).

## Preparation
### RSJoin
```shell
# build RSJoin/RSJoin_opt
cd RSJoin
mkdir release
cd release
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build .
```

### SJoin
```shell
# clone SJoin
git clone git@github.com:InitialDLab/SJoin.git
# apply the modifications
cp -rf SJoinMod/* ./SJoin
# build SJoin
cd SJoin/sjoin
mkdir release
cd release
CXXFLAGS=-O2 CPPFLAGS=-DNDEBUG ../configure
make
# build TPC-DS data preprocess in SJoin
cd ../../tpcds_data_proc
make
```

### Data
#### Graph
1. Download the Epinions dataset
```shell
curl -O https://snap.stanford.edu/data/soc-Epinions1.txt.gz > /dev/null 2>&1
gzip -d soc-Epinions1.txt.gz
tail -n +5 soc-Epinions1.txt > epinions.txt
rm -f soc-Epinions1.txt
sed -i "s/\t/,/g" epinions.txt
```
2. Modify the `inputFile` and `outputDir` in `Utils/GraphDataPreprocess.scala`. Then run
```shell
scala -J-Xmx200g -J-Xms200g ./Utils/GraphDataPreprocess.scala
```

#### TPC-DS
1. Download the TPC-DS tool from the [website](https://www.tpc.org/) and unzip.
2. Run `make` under the `$tpc-ds-tool/tools/` folder. ($TPC-DS-tool is the extracted folder)
3. Run `$tpc-ds-tool/tools/dsdgen` [your options] to generate TPC-DS data
4. Run `create_qx_data $tpc-ds-tool/tools/ qx_sf10.dat` in `SJoin/tpcds_data_proc/`
5. Run `create_qy_data` and `create_qz_data`

#### LDBC-SNB
1. Clone LDBC SNB Datagen from git@github.com:ldbc/ldbc_snb_datagen_spark.git
2. Build the Datagen following the instructions
3. Run the following
```shell
PLATFORM_VERSION=$(sbt -batch -error 'print platformVersion')
DATAGEN_VERSION=$(sbt -batch -error 'print version')
LDBC_SNB_DATAGEN_JAR=$(sbt -batch -error 'print assembly / assemblyOutputPath')
./tools/run.py --parallelism 1 --memory 180g -- --format csv --scale-factor 1 --mode raw
```
4. Modify the `inputDir` and `outputFile` in `Utils/SNBDataPreprocess.scala`. Then run
```shell
scala -J-Xmx200g -J-Xms200g ./Utils/SNBDataPreprocess.scala
```

## Experiments
### RSJoin
1. Modify the `$data_path` in `RSJoin/scripts/run_*.sh`
2. Run `RSJoin/scripts/run_rsjoin.sh` and `RSJoin/scripts/run_rsjoin_opt.sh`
3. Run `RSJoin/scripts/run_line3_input_size.sh`
4. Run `RSJoin/scripts/run_line3_sample_size.sh`
5. Run `RSJoin/scripts/run_line4_update_time.sh`
6. Run `RSJoin/scripts/run_qz_scale_factor.sh`
7. Run `RSJoin/scripts/run_qz_fk_scale_factor.sh`

### SJoin
1. Modify the `$data_path` in `SJoin/scripts/run_*.sh`
2. Run `SJoin/scripts/run_sjoin.sh` and `SJoin/scripts/run_sjoin_opt.sh`
3. Run `SJoin/scripts/run_line3_input_size.sh`
4. Run `SJoin/scripts/run_line3_sample_size.sh`
5. Run `SJoin/scripts/run_line4_update_time.sh`
6. Run `SJoin/scripts/run_qz_fk_scale_factor.sh`

### RSWP
1. Modify the `$data_path` in `RSJoin/scripts/run_predicate*.sh`
2. Run `RSJoin/scripts/run_predicate_density.sh` and `RSJoin/scripts/run_predicate_input_size.sh`

### Result
Collect the results from `run_*.out` files in the same folder.

### Build your own queries
#### Line-k joins and Star-k joins
Line-k joins and Star-k joins are supported for any k > 1. See [line_joins](RSJoin/scripts/run_line_joins.sh) and [star_joins](RSJoin/scripts/run_star_joins.sh).

#### General acyclic joins
You can implement acyclic joins using the [JoinTreeTemplate](RSJoin/include/acyclic/join_tree.h). See [Q10Algorithm](RSJoin/src/q10/q10_algorithm.cpp) as an example.