edht
=====

Distributed Hash Table in Erlang.

Build
-----

    $ rebar3 compile

Run Server
----------

    $ rebar3 shell

Run Unit Tests
--------------

    $ rebar3 eunit


Run cluster
-----

- Create a python3 virtualenv `edht`.
- `pip install -r requirements.txt`
- `python cluster.py 5`


TODO
----
- [ ] Error handling in general
- [ ] Meaningful response for bad client requests
- [ ] Specify bind address for node listeners in config file
