### install deps on centos7

```bash
sudo yum -y install epel-release
sudo yum install git gcc zlib-devel bzip2-devel readline-devel sqlite-devel openssl-devel
sudo yum install libffi-devel
```

### install pyenv

```bash
git clone https://github.com/pyenv/pyenv.git $HOME/.pyenv
```

```bash
nano $HOME/.bashrc
```

```bash
## pyenv configs
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"

if command -v pyenv 1>/dev/null 2>&1; then
  eval "$(pyenv init -)"
fi
```

```bash
source $HOME/.bashrc
```

### install python 3

```bash
pyenv install 3.7.3
pyenv local 3.7.3
```
