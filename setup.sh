#!/bin/bash

## Install the Python build dependencies for Ubuntu
sudo apt update \
    && sudo apt install -y make \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    wget \
    curl \
    llvm \
    libncurses5-dev \
    libncursesw5-dev \
    xz-utils \
    tk-dev \
    libffi-dev \
    liblzma-dev \
    python-openssl \
    git \
    libxml2-dev \
    libxmlsec1-dev

## Install pyenv Python version manager
curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash \
&& echo '# enable pyenv' >> ~/.bashrc \
&& echo 'export PATH="~/.pyenv/bin:$PATH"' >> ~/.bashrc \
&& echo 'eval "$(pyenv init -)"' >> ~/.bashrc \
&& echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.bashrc \
&& source ~/.bashrc

## Install the latest stable version of Python
pyenv install 3.9.1

## Install poetry Python packaging and dependencies manager
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python

