# Third-party licenses

The production package uses only the Go standard library. Test and benchmark
dependencies are:

- `github.com/failsafe-go/failsafe-go` v0.9.6 - MIT License;
- `github.com/bits-and-blooms/bitset` v1.24.4 - MIT License (transitive);
- `go.uber.org/goleak` v1.3.0 - Apache-2.0 License.

The module graph and checksums in `go.mod` and `go.sum` are authoritative for
exact versions. Release gates regenerate the compiled dependency license
inventory and software bill of materials.
