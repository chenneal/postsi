## Introduction
We implemented two PostSI schedulers, one for a single server with an Shared-Everything (SE) architecture, one for MPP platforms based with an Shared-Nothing(SN) architecture. For comparison purpose, we also implemented two conventional SI schedulers for the SE and SN architectures respectively. You can view our source code in [GitHub](https://github.com/jlumqz/postsi.git). The code works only with the Linux operating system and the GCC compiler.


## The Code
You can download our code from the master branch of our postsi repository, simply using the command below:
```
git clone https://github.com/jlumqz/postsi.git
```

## Single-Server Installation

The single-machine versions are easy to install. You can just use the commands below to complete the installation.

### The Version of Conventional Snapshot Isolation

#### find the directory
```
cd postsi

cd stand-alone

cd SE_SI
```
#### compile && run
```
make

./SI
```

### The Version of Posterior Snapshot Isolation

The PostSI have the similar steps as above:

#### find the directory 
```
cd postsi

cd stand-alone

cd SE_PostSI
```
#### compile && run
```
make

./PSI
```

## MPP Installation 

Each distributed version is composed of a master program and a slave program:

+ The source code of the slave and the master are located in different directories. You need to compile them separately.

+ Before running the slave nodes, you must run the master node firstly. This order cannot be changed. Or the slave will not start normally.

+ You can run the two MPP versions either in a single-machine environment (using loop IP address) or in a cluster environment. To switch the environment, you need to configure the `config.txt` file.

### The Version of Conventional Snapshot Isolation

#### master

##### find the directory
```
cd postsi

cd distributed

cd SN_SI

cd SN_master
```
##### compile && run
```
make

./master
```

#### slave

##### find directory
```
cd postsi

cd distributed

cd SN_SI

cd SI_slave
```
##### compile && run
```
make

./slave
```

### The Version for Posterior Snapshot Isolation

#### master

##### find directory
```
cd postsi

cd distributed

cd SN_PostSI

cd PostSI_master
```
##### compile && run
```
make

./master
```

#### slave

##### find directory
```
cd postsi

cd distributed

cd SN_SI

cd PostSI_slave
```
##### compile && run
```
make

./slave
```

Details can be found in the README.md of the GitHub repository.  

Have fun :)