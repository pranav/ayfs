# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure(2) do |config|
  config.vm.synced_folder "./", "/vagrant"
  config.vm.box = "digital_ocean"

  config.ssh.private_key_path = "~/.ssh/digital_ocean"
  config.vm.provider :digital_ocean do |provider, override|
          override.ssh.private_key_path = '~/.ssh/digital_ocean'
          override.vm.box = 'digital_ocean'
          override.vm.box_url = "https://github.com/smdahlen/vagrant-digitalocean/raw/master/box/digital_ocean.box"
          provider.token = '06afa880e5c6696b4e9a7eebbd61a2e62d37d9b8f1df9183594e7fc812e4a6b4'
          provider.image = 'ubuntu-14-04-x64'
          provider.region = 'nyc2'
          provider.size = '512mb'
  end

  config.vm.define "node4" do |node4_config|
    node4_config.vm.network "private_network", ip: "10.0.0.2"
    node4_config.vm.hostname = "node4"
    node4_config.vm.provision "shell", path: "provisioners/node.sh"
  end

  config.vm.define "node6" do |node4_config|
    node4_config.vm.network "private_network", ip: "10.0.0.2"
    node4_config.vm.hostname = "node6"
    node4_config.vm.provision "shell", path: "provisioners/node.sh"
  end



  config.vm.define "node5" do |node5_config|
    node5_config.vm.network "private_network", ip: "10.0.0.3"
    node5_config.vm.hostname = "node5"
    node5_config.vm.provision "shell", path: "provisioners/node.sh"
  end

  config.vm.define "node3" do |node3_config|
    node3_config.vm.network "private_network", ip: "10.0.0.6"
    node3_config.vm.hostname = "node3"
    node3_config.vm.provision "shell", path: "provisioners/node.sh"
  end

  config.vm.define "node4" do |node4_config|
    node4_config.vm.network "private_network", ip: "10.0.0.7"
    node4_config.vm.hostname = "node4"
    node4_config.vm.provision "shell", path: "provisioners/node.sh"
  end


  config.vm.define "etcd" do |etcd_config|
    etcd_config.vm.network "private_network", ip: "10.0.0.4"
    etcd_config.vm.hostname = "etcd"
    etcd_config.vm.provision "shell", path: "provisioners/etcd.sh"
  end

  config.vm.define "client" do |client_config|
    client_config.vm.network "private_network", ip: "10.0.0.5"
    client_config.vm.hostname = "client"
    client_config.vm.provision "shell", path: "provisioners/node.sh"
  end

end
