## Introduction
We implemented the PostSI schedulers in both Single-machine with Shared-Everything(SE) architectures and MPP platform with Shared-Nothing(SN) architectures, and for comparison purpose, we implemented two conventional SI schedulers for the SE and SN architectures respectively. To run the code, Linux operating system and GCC compiler are needed. You can view our code in [GitHub](https://github.com/jlumqz/postsi.git)

## Get the code
You can get our code from the master branch of our postsi repository. Simply use the single command below:
```
git clone https://github.com/jlumqz/postsi.git
```

## Stand-alone Installation

The single-machine version is easy to install, you only need find the right directory and compile the source code. You can just use a few command below to complete the installation：

### SI Version

find directory 
```
cd postsi

cd stand-alone

cd SE_SI
```
compile && run
```
make

./SI
```

### PostSI Version

The PostSI version have the similar steps as above:

find directory 
```
cd postsi

cd stand-alone

cd SE_PostSI
```
compile && run
```
make

./PSI
```

## Distributed Installation 

Before install the distributed versions, you'd better know the list below:

+ Two versions both have two directory of slave and the master, which contains the source code file, you should compile them separately.

+ Before run the slave nodes, you should run the master node first. The order here should never be changed, otherwise the slave can not start-up normally.

+ You can run the two versions of distributed system either in stand-alone environment( use loop IP address ) or in cluster environment. The Only thing you need to do is to configure the `config.txt`.

### SI Version

#### master

find directory
```
cd postsi

cd distributed

cd SN_SI

cd SN_master
```
complie && run
```
make

./master
```

#### slave

find directory
```
cd postsi

cd distributed

cd SN_SI

cd SI_slave
```
complie && run
```
make

./slave
```

### PostSI Version

#### master

find directory
```
cd postsi

cd distributed

cd SN_PostSI

cd PostSI_master
```
complie && run
```
make

./master
```

#### slave

find directory
```
cd postsi

cd distributed

cd SN_SI

cd PostSI_slave
```
complie && run
```
make

./slave
```

If you are confused in details, please look up the README.md in respective directory. 

Have fun :)