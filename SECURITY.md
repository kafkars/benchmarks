# Security policy

## Supported versions

There is no published release. Nothing from this repository goes to a package
registry, and there is no version line to backport to. Security fixes are made
on `main`.

## Reporting a vulnerability

Do not open a public issue. Email `shawn@zsumz.com` with
`[kafka-benchmarks security]` in the subject. Once the public repository exists,
its private GitHub Security Advisory form is the preferred channel.

Include the affected revision, the smallest practical reproducer, the security
impact, and any known mitigations. Do not include live credentials, broker
endpoints you do not own, or third-party private data.

You should receive an acknowledgement within three business days and an initial
triage assessment within seven. Those are coordination goals, not a
service-level agreement: this is a small project, and stating a target honestly
is more useful than promising a response time nobody is staffed to guarantee.

Please allow a reasonable remediation and coordinated-disclosure window. The
maintainer will credit reporters who want attribution and will keep reporters
informed when a fix or advisory is ready.

## Scope

In scope: the control plane, the adapters and the shim in this repository, the
scripts, the evidence-sealing path, and the CI workflows. The interesting
failure modes here are integrity rather than confidentiality — anything that
lets a bundle claim an experiment, an environment, a digest, or a verification
result that does not match what actually ran is a security issue even when no
secret is exposed. So is anything that causes the harness to execute a command
it was not configured with, including through a crafted scenario file or a
crafted adapter response.

Out of scope for this repository, and better reported to their own maintainers:
Apache Kafka broker vulnerabilities, librdkafka, third-party dependencies, and
the sibling `kafka-client`, `kafka-driver`, and `kafka-protocol` repositories.
Benchmark results themselves are not confidential, and a bundle sealed on a
developer machine is expected to record that machine's hostname and toolchain.
