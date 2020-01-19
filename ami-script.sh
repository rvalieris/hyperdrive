#!/bin/bash
set -e
cd /root
curl https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o conda.sh
mkdir /opt/conda
bash conda.sh -bfp /opt/conda
rm -f conda.sh
ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh
source /etc/profile.d/conda.sh
echo -e "export LC_ALL=C\nexport LANG=C" > /etc/profile.d/lang.sh
source /etc/profile.d/lang.sh
conda update --yes --all -c conda-forge -c bioconda
conda install --yes -c conda-forge -c bioconda awscli snakemake inotify_simple
conda clean --all --yes
yum -y install git gcc libuuid-devel openssl-devel libseccomp-devel squashfs-tools cryptsetup glibc-minimal-langpack
yum -y update

curl https://dl.google.com/go/go1.13.6.linux-amd64.tar.gz -o go.tar.gz
tar xf go.tar.gz
rm -f go.tar.gz
export HOME=/root
export VERSION=v3.5.2
export GOPATH=/root/gopath
export PATH=/root/go/bin:$PATH
go get -d github.com/sylabs/singularity || true
cd $GOPATH/src/github.com/sylabs/singularity
git fetch
git checkout $VERSION
go version
bash mconfig
make -C builddir
make -C builddir install
cd /root

# disable stuff
systemctl mask serial-getty@ttyS0.service
systemctl disable getty@tty1
systemctl disable rsyslog

# clean up
yum -y remove gcc git postfix glibc-locale-source mariadb-libs glibc-all-langpacks selinux-policy cronie audit
yum -y autoremove
yum clean all
rm -rf /usr/share/doc
rm -rf $GOPATH
rm -rf /root/go
rm -rf /root/.cache
rm -rf /var/cache/yum/

# reset log file
cp /var/log/cloud-init-output.log /root/ami.log
df -h
echo -n > /var/log/cloud-init-output.log
