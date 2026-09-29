# gen

Optional `protoc-gen-go` output — `buf generate --template proto/buf.gen.yaml`
writes here with `paths=source_relative`. Never hand-edit.

**Nothing in this SDK imports it.** `../contract/` is a hand-written,
dependency-free protobuf codec for the same wire format, so `go build`,
`go vet` and `go test` all work in a fresh checkout with this directory empty
(or absent). The directory is gitignored.

The `managed.override.go_package_prefix` entry in `proto/buf.gen.yaml` is what
makes generation succeed at all: Go has no default in buf managed mode, so
without it `protoc-gen-go` fails on a missing `go_package` and aborts the
whole `buf generate` run, silently producing nothing.

For local regeneration without remote plugins: `bash scripts/gen-proto.sh`.
