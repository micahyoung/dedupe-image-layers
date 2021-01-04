# shrink an OCI image with hardlinks

## Usage
```
go run main.go -source-ref golang:1.15-alpine -destination-ref golang:1.15-alpine-flat 
```
use `-remote` to operate entirely on a registry without a Docker daemon
