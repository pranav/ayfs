# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure(2) do |config|
  config.vm.synced_folder "./", "/vagrant"
  config.vm.box = "ubuntu/trusty64"
  #config.vm.network "public_network"
  config.vm.provider "virtualbox" do |vb|
    vb.gui = false
    vb.memory = "1024"
  end

  config.vm.define "node1" do |node1_config|
    node1_config.vm.network "private_network", ip: "10.0.0.2"
    node1_config.vm.provision "shell", path: "provisioners/node.sh"
  end

  config.vm.define "node2" do |node2_config|
    node2_config.vm.network "private_network", ip: "10.0.0.3"
    node2_config.vm.provision "shell", path: "provisioners/node.sh"
  end

  config.vm.define "etcd" do |etcd_config|
    etcd_config.vm.network "private_network", ip: "10.0.0.4"
    etcd_config.vm.provision "shell", path: "provisioners/etcd.sh"
  end

end
