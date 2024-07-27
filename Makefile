.PHONY: upgrade-reqless
upgrade-reqless:
	git -C lib/qless/reqless-core fetch
	git -C lib/qless/reqless-core checkout origin/main
	make reqless-core

.PHONY: reqless-core
reqless-core:
	# Ensure reqless-core is built
	make -C lib/qless/reqless-core/
	cp lib/qless/reqless-core/reqless.lua lib/qless/lua/
	cp lib/qless/reqless-core/reqless-lib.lua lib/qless/lua/
