.PHONY: upgrade-reqless
upgrade-reqless:
	git -C lib/reqless/reqless-core fetch
	git -C lib/reqless/reqless-core checkout origin/main
	make reqless-core

.PHONY: reqless-core
reqless-core:
	# Ensure reqless-core is built
	make -C lib/reqless/reqless-core/
	cp lib/reqless/reqless-core/reqless.lua lib/reqless/lua/
	cp lib/reqless/reqless-core/reqless-lib.lua lib/reqless/lua/
