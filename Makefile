.DEFAULT_GOAL := build

BINARY_NAME = svc
BUILD_PATH = cmd/build

build:
	mkdir -p $(BUILD_PATH)
	CGO_ENABLED=0 go build -o $(BUILD_PATH)/$(BINARY_NAME) cmd/main.go

run: build
	$(BUILD_PATH)/$(BINARY_NAME)

clean:
	rm -rf $(BUILD_PATH)
