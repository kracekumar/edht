# -*- mode: ruby -*-

def command?(name)
  `which #{name} > /dev/null 2>&1`
  $?.success?
end

if Vagrant::VERSION == "1.8.7" then
    path = `which curl`
    if path.include?('/opt/vagrant/embedded/bin/curl') then
        puts "In Vagrant 1.8.7, curl is broken. Please use Vagrant 1.8.6 "\
             "or run 'sudo rm -f /opt/vagrant/embedded/bin/curl' to fix the "\
             "issue before provisioning. See "\
             "https://github.com/mitchellh/vagrant/issues/7997 "\
             "for reference."
        exit
    end
end

servers = []
for x in 3.times do
  server = {
    :hostname => "edht-" + x.to_s,
    :ip => "192.168.0." + (x + 1).to_s
  }
  servers.push(server)
end


Vagrant.configure(2) do |config|

  # For LXC. VirtualBox hosts use a different box, described below.
  servers.each do |machine|
    config.vm.define machine[:hostname] do |node|
      node.vm.network "private_network", ip: machine[:ip], lxc__bridge_name: 'vlxcbr1'
      node.vm.synced_folder ".", "/srv/edht"
      node.vm.box = "fgrehm/trusty64-lxc"

      # Specify LXC provider before VirtualBox provider so it's preferred.
      node.vm.provider "lxc" do |lxc|
        if command? "lxc-ls"
          LXC_VERSION = `lxc-ls --version`.strip unless defined? LXC_VERSION
          if LXC_VERSION >= "1.1.0"
            # Allow start without AppArmor, otherwise Box will not Start on Ubuntu 14.10
            # see https://github.com/fgrehm/vagrant-lxc/issues/333
            lxc.customize 'aa_allow_incomplete', 1
          end
          if LXC_VERSION >= "2.0.0"
            lxc.backingstore = 'dir'
          end
        end
      end
    end
  end

$provision_script = <<SCRIPT
set -x
set -e
set -o pipefail
# If the host is running SELinux remount the /sys/fs/selinux directory as read only,
# needed for apt-get to work.
if [ -d "/sys/fs/selinux" ]; then
  sudo mount -o remount,ro /sys/fs/selinux
fi
/srv/edht/linux_setup.sh | sudo tee -a /var/log/edht_provision.log
SCRIPT

  config.vm.provision "shell",
    # We want provision.py to be run with the permissions of the vagrant user.
    privileged: false,
    inline: $provision_script
end
