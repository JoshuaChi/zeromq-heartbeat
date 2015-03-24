PROJECT = wio

RELX=./relx

DEPS = sync erlzmq2
dep_erlzmq2 = git git://github.com/zeromq/erlzmq2.git master
dep_sync = git git://github.com/rustyio/sync.git master


 
.PHONY: release clean-release
 
release: clean-release all 
	$(RELX) -o rel/$(PROJECT)
 
clean-release: 
	rm -rf rel/$(PROJECT)
 
include erlang.mk