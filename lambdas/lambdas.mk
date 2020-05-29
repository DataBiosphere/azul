
.PHONY: config
config: .chalice/config.json

.PHONY: local
local: check_python config
	chalice local

.PHONY: clean
clean: check_env
	git clean -Xdf
