# Teamone Azkaban
基于 Azkaban 3.91.0 版本，新增或修改支持通过Http方式调用新增、修改、删除、编辑job 或 flow 或 project等操作，用于支撑霆万调度中心

# 主要特性
1. 支持通过接口调用的方式创建，删除project
2. 支持通过接口调用的方式创建，修改，删除flow，支持定时调度 flow 中的部分任务(非全部)，支持停止flow中的部分任务(非全部)
3. 支持通过接口调用的方式创建，修改，删除job，同时支持新增时设置依赖关系，删除时自动挂载到上级依赖
4. 支持 http 类型的job，该类型由 Teamone Azkaban Http plugin 提供，支持设置http任务，Azkaban部署所在服务器作为客户端进行 http 调用，其他特征和 command job 一致
5. 修正了部分 Azkaban 本身的bug，如新增job初始化时空指针等错误

# 安装方式

执行如下命令
```shell
./gradlew build installDist -x test
```
即可得到
azkaban-exec-server.tar.gz
azkaban-web-server.tar.gz

然后上传到服务器进行设置和部署


其他可参考官方文档：
# Azkaban 

[![Build Status](https://travis-ci.com/azkaban/azkaban.svg?branch=master)](https://travis-ci.com/azkaban/azkaban)[![codecov.io](https://codecov.io/github/azkaban/azkaban/branch/master/graph/badge.svg)](https://codecov.io/github/azkaban/azkaban)[![Join the chat at https://gitter.im/azkaban-workflow-engine/Lobby](https://badges.gitter.im/azkaban-workflow-engine/Lobby.svg)](https://gitter.im/azkaban-workflow-engine/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)[![Documentation Status](https://readthedocs.org/projects/azkaban/badge/?version=latest)](https://azkaban.readthedocs.org/en/latest/?badge=latest)


## Build
Azkaban builds use Gradle and requires Java 8 or higher.

The following set of commands run on *nix platforms like Linux, OS X.

```
# Build Azkaban
./gradlew build installDist -x test

# Clean the build
./gradlew clean

# Build and install distributions
./gradlew installDist

# Run tests
./gradlew test

# Build without running tests
./gradlew build -x test
```

### Build a release

Pick a release from [the release page](https://github.com/azkaban/azkaban/releases). 
Find the tag corresponding to the release.

Check out the source code corresponding to that tag.
e.g.

`
git checkout 3.30.1
`

Build 
```
./gradlew clean build
```

## Documentation

The current documentation will be deprecated soon at [azkaban.github.io](https://azkaban.github.io). 
The [new Documentation site](https://azkaban.readthedocs.io/en/latest/) is under development.
The source code for the documentation is inside `docs` directory.

For help, please visit the [Azkaban Google Group](https://groups.google.com/forum/?fromgroups#!forum/azkaban-dev).

## Developer Guide

See [the contribution guide](https://github.com/azkaban/azkaban/blob/master/CONTRIBUTING.md).

#### Documentation development

If you want to contribute to the documentation or the release tool (inside the `tools` folder), 
please make sure python3 is installed in your environment. python virtual environment is recommended to run these scripts.

To create a venv & install the python3 dependencies inside it, run

```bash
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
```
After, enter the documentation folder `docs` and make the build by running
```bash
cd docs
make html
```

Find the built docs under `_build/html/`.

For example on a Mac, open them in browser with:

```bash
open -a "Google Chrome" _build/html/index.html
```

**[July, 2018]** We are actively improving our documentation. Everyone in the AZ community is 
welcome to submit a pull request to edit/fix the documentation.
