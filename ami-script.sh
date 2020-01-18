#!/bin/bash
cd /root
curl https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o a.sh
mkdir /opt/conda
bash a.sh -bfp /opt/conda
rm -f a.sh
ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh
source /etc/profile.d/conda.sh
echo 'conda activate base' >> /home/ec2-user/.bashrc
echo -e "export LC_ALL=C\nexport LANG=C" > /etc/profile.d/lang.sh
conda install --yes -c conda-forge -c bioconda awscli snakemake inotify_simple
conda update --yes --all -c conda-forge -c bioconda
yum -y install git gcc libuuid-devel openssl-devel libseccomp-devel squashfs-tools cryptsetup glibc-minimal-langpack
yum -y update

curl https://dl.google.com/go/go1.13.6.linux-amd64.tar.gz -o go.tar.gz
tar xf go.tar.gz
rm -f go.tar.gz
export HOME=/root
export VERSION=v3.5.2
export GOPATH=/root/gopath
export PATH=/root/go/bin:$PATH
go get -d github.com/sylabs/singularity
cd $GOPATH/src/github.com/sylabs/singularity
git fetch
git checkout $VERSION
go version
bash mconfig
make -C builddir
make -C builddir install
cd /root

# clean up
sudo yum -y remove gcc git postfix glibc-locale-source mariadb-libs glibc-all-langpacks
sudo yum -y autoremove
rm -rf /usr/share/doc
rm -rf $GOPATH
rm -rf /root/go
rm -rf /root/.cache
conda clean --all --yes
yum clean all
rm -rf /var/cache/yum/

# reset log file
cp /var/log/cloud-init-output.log /root/ami.log
echo -n > /var/log/cloud-init-output.log
