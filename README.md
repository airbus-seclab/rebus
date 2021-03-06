# REbus

REbus facilitates the coupling of existing tools that perform specific tasks,
where one's output will be used as the input of others.

A few task examples:

* compute file hashes (md5, sha1)
* apply file identification tools (libmagic, peid, antivirus)
* extract files from archives
* extract printable strings from files
* nmap scans
* fetch network service banners
* fingerprint an SSL service to detect weak ciphersuites or configuration

## Very quick start

Run the REbus docker image

```bash
$ docker run -ti iwseclabs/rebus
```


## Quick start

Dependencies:

* python2.7
* mercurial (to interact with this repository only)
* setuptools
* python2-tornado >= 3.0
* dbus
* python2-dbus
* python2-gobject
* dbus-glib

Optional dependencies:

* pip install larch-pickle
* python2-pika (when rabbitmq bus is used) - version 0.10 at least

To install these dependencies on Arch Linux:

```bash
$ pacman -S mercurial python2 python2-setuptools python2-dbus python2-gobject2 dbus-glib python2-tornado python2-magic
```


To install these dependencies on Debian Jessie:

```bash
$ apt-get install mercurial python-setuptools python-dbus python-gobject dbus-x11 python-magic python-tornado
```

To install these dependencies on Ubuntu 15.04 (vivid) :

```bash
$ apt-get install mercurial python-setuptools python-dbus python-gobject dbus-x11 python-tornado python-magic
```

Quick installation & test:

```bash
$ git clone https://github.com/airbus-seclab/rebus
$ cd rebus
$ python2 ./setup.py install

# list available agents
$ rebus_agent --list-agents

# If DBus is not already running on your machine, or on a headless machine.
# Then run all commands in separate screen tabs.
$ dbus-launch screen
# run the bus master in terminal 1
$ rebus_master dbus

# run a few agents, run each command in separate terminals
$ rebus_agent --bus dbus web_interface
$ rebus_agent --bus dbus unarchive

# open a web browser on http://localhost:8080/
$ firefox http://localhost:8080/
```

The use of virtualenv is recommended to maintain an isolated Python
environment, where any package required by agents can be installed. On
Archlinux, the following commands may be used for installation:

```bash
$ virtualenv2 --system ~/rebus-virtualenv
$ . ~/rebus-virtualenv/bin/activate
$ git clone https://github.com/airbus-seclab/rebus
$ cd rebus
$ python ./setup.py develop
# the use of develop instead of install removes the need to re-run
# "setup.py install" every time a change is made
```


## Components overview

* Descriptors_ are REbus messages, conveyed to and from agents
* Agents wrap external tools, process and generate descriptors
* the `Communication Bus`_ lets agents communicate
* the Storage_ circulates and stores descriptors

### Descriptors
Descriptors store agents' outputs and inputs as Python objects.

Descriptors have several properties:

* **selector** describes data type (≃ MIME type), contains unique identifier,
  e.g. ``/signature/md5/%6e1d5169661a50(...)f989129a583f92b9dee``
* **label** human name the descriptor relates to
* **uuid** group descriptors related to the same analysed object
* **value** descriptor value
* **precursors** list of parents' selectors
* **agent name** agent that produced this descriptor
* **domain** separates analyses
* **version** integer version if descriptor is updated
* **processing time** time taken to compute this descriptor

Descriptors may perform several actions:

* spawn new descriptor in the same UUID zone
* spawn new version of the same descriptor
* create link (linktype) descriptor
* serialization

### Communication Bus
The Bus API performs the following duties:

* allows agents to push Descriptors_
* broadcasts new Descriptors_' selectors to every agent (1st stage descriptor
  filtering by agents)
* allows agents to filter new descriptors based on their metadata and value
  (2nd stage filtering)
* keeps track of which agents have processed each descriptor
* allows several instances of one agent may run simultaneously
* conveys requests through the storage API

Three communication buses have been implemented:

* **LocalBus**: combine *n* agents to create a new composite command-line tool
* **DBusBus**: sends messages over DBus.
* **RabbitBus**: sends messages over RabbitMQ.

When using any of the last two buses, agents are not stopped as soon as
processing is finished, which enables user to interact with the bus (ex. use
the web interface, inject descriptors interactively)

#### LocalBus
This bus implementation runs Agents as several threads in a single process.

The bus exits once every agent has finished carrying out its duties (ex.
injecting files, processing descriptors).

The run() method of every agent is run in a separate threads. For agents that
do not override run(), the thread exits immediately. Processing is performed in
threads that have defined this method, typically inject. As agents might not be
thread-safe (ex. due to the use of non-thread-safe features of Python objects,
non-thread-safe C bindings...), you might want to refrain from using several
"inject" agents.

#### DBusBus
This bus implementation uses DBus as a communication mechanism between the *bus
master* and the agents, to send messages bus and perform remote procedure
calls.

This bus implementation runs Agents as separate processes.

The bus exits when the *bus master* process gets stopped by the user.

Agents may be run remotely, by connecting to a remote DBus server.

This bus allows using the web interface.

This implementation does not support messages over ~134MB.

#### RabbitBus
This bus implementation uses RabbitMQ as a communication mechanism between the *bus master* and the agents.

This bus implementation runs Agents as separate processes.

The bus exits when the *bus master* process gets stopped by the user.

This implementation as a few rough edges (known bugs):

* Queues must not exist, or be empty before the bus master and agents is
  launched. Leftover messages from previous runs cause bugs.
* Only 10000 agents may connect to the bus master during its lifetime.
* At shutdown, depending on how it was initiated, the *bus master* and/or
  agents sometimes enter an infinite loop and must be killed.

These bugs should be resolved in future versions. Nevertheless, this bus
implementation is deemed reliable.



### Storage
The storage API provides the following services to agents:

* find descriptor by selector regex
* find descriptor by uuid
* load/store agent internal state (bus resuming)
* mark descriptor as processed, list unprocessed descriptors

Two storage backends have currently been implemented:

* RAMStorage: stored data is forgotten when the bus exits
* Diskstorage: stores data as files. The bus may be stopped and resumed later

### Agents
Agents process Descriptors_, and usually act as an interface between the
`Communication Bus`_ and external tools.

Some agents are short-lived as they perfom only one task, then exit. The
**inject** agent or **ls** agent are such examples. These agents override the
**run()** method.

Most agents process Descriptors_ they are interested in. These agents override
the **process()** and/or **bulk_process()** methods.

#### Operation Modes
Agents that use Descriptors_ as input override the **process()** and/or
**bulk_process()** methods. These methods are called by the `Communication
Bus`_ according to the Operation Mode this agent is in.

By default, agents support all operation modes. The list of supported operation
modes as well as the default operation mode can be overridden by defining the
*_operationmodes_* attribute in an Agent.

##### Automatic operation mode
Agents that run in automatic mode process every descriptor they are interested
in as soon as they are received, provided the agent is not busy processing
another Descriptor at that time.

##### Interactive operation mode
Agents that run in interactive mode indicate to the `Communication Bus`_ that
they are able to process `Descriptors`_ they are interested in. The actual
processing is performed whenever the `Communication Bus`_ requests it, usually
when the user has requested it.

##### Idle operation mode
Agents that run in idle operation mode indicate to the `Communication Bus`_
that they are able to process `Descriptors`_ they are interested in. These
descriptors get processed when the `Communication Bus`_ indicates that all
`Descriptors`_ have been processed or marked processable; the bus is said to be
"idle" at that time.

To receive all such descriptors at once, agents may override the bulk_process
method.


### Provided agents

A few agents are provided with REbus. Their purpose is to assist using the bus,
not to perform any data analysis.

* **inject** inject local files in the bus
* **httplistener** inject Descriptors_ from HTTP POST requests
* **ls** list descriptors
* **unarchive** recursively extract archives, inject contained files
* **return** output descriptors to stdout if selector matches regexp
* **link_finder** find link between descriptors, e.g.\ same file type
* **link_grapher** create graphs from links between descriptors
* **dotrenderer** rendering dot to svg
* **web_interface** web interface

### Rebus Infrastructure launcher (bin/rebus_infra)

rebus_infra stands for REbus infractructure. This script deploys a REbus bus
and its agents based on a configuration file.

```bash
$ rebus_infra configuration_file.yaml
```

This file must contain at least a 'master' and an 'agents' section at the
root of the document.

Those two sections contain the following attributes:

* **Master Section**

  - **bus** : 'localbus', 'dbus' or 'rabbit'
  - **logfile** : The logfile's path
  - **verbose_level** : Verbosity level for this agent, between 0 and 3
  - **storage** : 'ramstorage' or 'diskstorage'
* **Agents Section**

  - **busaddr** : Address of the dbus bus
  - **modules** : All the (Python) modules to load
  - **stages** : Describes all the stages in the execution order

    - **agents** : list of agents to execute in the stage in the execution order
    -  # == *alternative 1* ==
    - **agent_name** : 'parameters'
    -  # == *alternative 2* ==
    - **agent_name** :

      - **params**: parameters
      - **verbose_level**: Verbosity level for this agent, between 0 and 3

Here is an example of a configuration file for 'rebus_infra'

```yaml
# =================== Bus Master =============================

master:
  bus: dbus
  logfile: /tmp/rebus_master.log
  verbose_level: 0

# ===================  Agents ===============================

agents:
  #busaddr: unix:abstract=/tmp/dbus-muyzQoNsLE
  modules:
  #rebus_demo.agents
  stages:
    - agents:
        - inject:
            params: /bin/bash /bin/ls
            verbose_level: 3
        - inject: /bin/cp
    - agents:
        - ls:
            params: "/binary/elf"
            verbose_level: 3
```


```bash
 $ rebus_infra -f /etc/rebus/rebus-infra-config-example.yaml
INFO:rebus:Starting stage 0
INFO:rebus.bus.dbus:Agent inject registered with id inject-:1.149 on domain default
INFO:rebus.agent:[inject-:1.149] Agent inject registered on bus dbus with id inject-:1.149
INFO:rebus.agent:[inject-:1.149] Restore state: ''
DEBUG:rebus.agent:[inject-:1.149] pushed default:/binary/elf/%da92956d6f6e4068af9cc82a1d52d897201dd2a89e3786cbe6173190ad9e604c(bash)=[2307233]['\x7fELF\x02\x01\x01\x...], not already present: True
DEBUG:rebus.agent:[inject-:1.149] pushed default:/binary/elf/%13eab6034d09927e519835e2cfad225f43c204c60f522f492e2b796a0877011c(ls)=[358061]['\x7fELF\x02\x01\x01\x...], not already present: True
INFO:rebus:Starting stage 0
INFO:rebus.bus.dbus:Agent ls registered with id ls-:1.150 on domain default
INFO:rebus.agent:[ls-:1.150] Agent ls registered on bus dbus with id ls-:1.150
INFO:rebus.agent:[ls-:1.150] Restore state: ''
/binary/elf/%13eab6034d09927e519835e2cfad225f43c204c60f522f492e2b796a0877011c
/binary/elf/%da92956d6f6e4068af9cc82a1d52d897201dd2a89e3786cbe6173190ad9e604c
/binary/elf/%258f4cde050970ecbc039c0f250ac30d97d8b966dab029bfd135090b0101ac16
```


## Building the documentation

This documentation is generated automatically, based only (for now) on source
code comments.

```bash
$ cd rebus/doc
```

Make sure sphinx-apidoc2 and sphinx-build2 are installed on your system.
Override the SPHINXBUILD and SPHINXAPIDOC if the executables are named
differently.

```bash
$ make generate
```

This will overwrite .rst files in source/.

```bash
$ make singlehtml
```

## REbus resources
REbus has been presented at SSTIC 2015. Slides and video (in french) can be
found at https://www.sstic.org/2015/presentation/rebus/

## Licence

REbus is released under a BSD 2-clause licence.
