.POSIX:

CRYSTAL = crystal

test: .phony
	$(CRYSTAL) run test/*_test.cr test/parser/*_test.cr -- --parallel 4

run:
	$(CRYSTAL) run src/csv_manager.cr

.phony:
