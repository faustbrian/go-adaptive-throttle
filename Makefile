GOLIB ?= golib

.PHONY: check ci cohesion inventory repository-check workflows

check:
	$(GOLIB) check --all

ci:
	$(GOLIB) repository check
	$(GOLIB) workflows check
	$(GOLIB) cohesion check
	$(GOLIB) check --all

cohesion:
	$(GOLIB) cohesion check

inventory:
	$(GOLIB) inventory

repository-check:
	$(GOLIB) repository check

workflows:
	$(GOLIB) workflows check
