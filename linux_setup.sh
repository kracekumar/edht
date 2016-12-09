echo "Starting setup"
sudo apt-get update
sudo apt-get install -y build-essential libssl-dev libcurl4-gnutls-dev libexpat1-dev gettext unzip git fish erlang erlang-doc libncurses5-dev
cd /tmp
install_erlang(){
    if hash erl 2 > /dev/null; then
        echo "erlang is setup"
    else
        wget -nc http://www.erlang.org/download/otp_src_19.1.tar.gz
        tar -zxf otp_src_19.1.tar.gz
        cd otp_src_19.1
        ./configure
        make
        sudo make install
    fi
}


install_rebar3(){
    if hash rebar3 2 > /dev/null; then
        echo "rebar3 installed"
    else
        cd ~
        mkdir -p bin
        cd bin
        git clone https://github.com/erlang/rebar3.git
        cd rebar3
        ./bootstrap
        echo "export PATH=~/bin/rebar3:$PATH" >> .bashrc
    fi
}

install_erlang
install_rebar3
