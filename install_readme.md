# install

## instal uv
# On macOS and Linux.
curl -LsSf https://astral.sh/uv/install.sh | sh

## install redis
sudo apt install redis-server，然后 sudo systemctl enable --now redis-server

## install postgres
sudo apt install postgresql postgresql-contrib
sudo systemctl enable --now postgresql


### set user and password

登录PostgreSQL
sudo -u postgres psql

新建用户
create user zyx;
分配权限
create user zyx superuser;
设置用户密码
create user zyx with password '123456'

修改登录PostgreSQL密码
ALTER USER postgres WITH PASSWORD 'postgres';

### add database

create database ainsight owner zyx;

alter datebase ainsight ower to zyx;

查看所有数据库
\l

查看所有用户
\du

### 编辑 PostgreSQL 配置允许外部访问
sudo nano /etc/postgresql/16/main/postgresql.conf
listen_addresses = '*'
<!-- sudo nano /etc/postgresql/16/main/pg_hba.conf
host all all 0.0.0.0/0 md5 -->
### 修改 PostgreSQL 端口号
sudo nano /etc/postgresql/16/main/postgresql.conf
port = 55432
### 重启
sudo systemctl restart postgresql
### 宿主机如何访问
wsl hostname -I
（WSL2 每次启动可能变化）。