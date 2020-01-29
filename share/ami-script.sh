#!/bin/bash
set -e
cd /root

# locale
echo -e "export LC_ALL=C\nexport LANG=C" > /etc/profile.d/lang.sh
source /etc/profile.d/lang.sh

# move ec2-user home to /tmp
unshare --user --map-root-user usermod -d /tmp/ec2-user ec2-user
mv /home/ec2-user /tmp

# remove stuff
yum -y remove postfix mariadb-libs selinux-policy cronie audit update-motd amazon-linux-extras gdisk libicu man-db less glibc-locale-source glibc-all-langpacks

# install conda
curl https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o conda.sh
bash conda.sh -bfp /opt/conda
rm -f conda.sh
ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh
source /etc/profile.d/conda.sh
conda update --all --yes -c conda-forge -c bioconda
conda clean --all --yes
conda install --yes -c conda-forge -c bioconda awscli snakemake inotify_simple
conda clean --all --yes

# install packages
yum -y install git gcc libuuid-devel openssl-devel libseccomp-devel squashfs-tools cryptsetup glibc-minimal-langpack mdadm
yum -y update

# install singularity
curl https://dl.google.com/go/go1.13.6.linux-amd64.tar.gz -o go.tar.gz
tar xf go.tar.gz
rm -f go.tar.gz
export GOCACHE=/media/ephemeral0/tmp
export HOME=/root
export VERSION=v3.5.2
export GOPATH=/media/ephemeral0/gopath
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
systemctl mask systemd-tmpfiles-clean.timer
systemctl disable getty@tty1
systemctl disable rsyslog

# dont use PrivateTmp
sed -i 's/PrivateTmp=yes/PrivateTmp=no/' /usr/lib/systemd/system/chronyd.service

# raid0 params
echo 'options raid0 default_layout=2' > /etc/modprobe.d/raid0.conf

# disable auto upgrade
sed -i 's/^repo_upgrade:.*/repo_upgrade: none/' /etc/cloud/cloud.cfg

# singularity
sed -i 's/^mount home =.*/mount home = no/' /usr/local/etc/singularity/singularity.conf
sed -i 's/^mount tmp =.*/mount tmp = no/' /usr/local/etc/singularity/singularity.conf
sed -i 's/^mount hostfs =.*/mount hostfs = yes/' /usr/local/etc/singularity/singularity.conf

# clean up
yum -y remove gcc git
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
