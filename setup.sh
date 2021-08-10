sudo yum -y update
sudo yum -y python3
sudo pip3 install pymongo
sudo pip3 install dnspython
sudo pip3 list
cp mongodb-org-5.0.repo  /etc/yum.repos.d/mongodb-org-5.0.repo
sudo yum install -y mongodb-mongosh
