PLATFORM ?= linux/amd64

# COLORS
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)


TARGET_MAX_CHAR_NUM=20

## Show help with `make help`
help:
	@echo ''
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk '/^[a-zA-Z\-_0-9]+:/ { \
		helpMessage = match(lastLine, /^## (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")-1); \
			helpMessage = substr(lastLine, RSTART + 3, RLENGTH); \
			printf "  ${YELLOW}%-$(TARGET_MAX_CHAR_NUM)s${RESET} ${GREEN}%s${RESET}\n", helpCommand, helpMessage; \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)

.PHONY: env
## Prepares env files and terraform tfvar files
env:
	./env_setup.sh

.PHONY: up
## Builds the base Docker image and starts Flink cluster
up:
	docker-compose up --build --remove-orphans

.PHONY: start
## Starts all services in docker-compose
start:
	docker-compose start

.PHONY: stop
## Stops all services in docker-compose
stop:
	docker-compose stop

.PHONY: tf-plan
## Plan the terraform actions
tf-plan:
	terraform -chdir=terraform/ plan

.PHONY: tf-apply
## Creates all necessary resources with terraform
tf-apply:
	terraform -chdir=terraform/ apply

.PHONY: tf-destroy
## creates all necessary resources with terraform
tf-destroy:
	terraform -chdir=terraform/ destroy