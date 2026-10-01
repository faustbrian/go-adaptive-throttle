# Security policy

## Supported versions

The latest stable v1 release receives security fixes. Older releases and the
`main` branch are unsupported; upgrade before reporting unless the issue is a
regression under active development.

| Version | Supported |
| --- | --- |
| Latest stable v1 release | Yes |
| Older releases | No |
| `main` | No |

## Reporting a vulnerability

Do not disclose a suspected vulnerability in a public issue. Use the
[private vulnerability reporting form](https://github.com/faustbrian/go-adaptive-throttle/security/advisories/new).

Do not include credentials, customer data, tenant identifiers, URLs, or raw
production errors in a public report or initial contact request.

Injected clocks, random sources, priority resolvers, overload classifiers,
resource identities, policy revisions, and observers are application trust
boundaries; review them before deployment.

The [threat model and accepted residual-risk dispositions](docs/operations.md#threat-model-and-accepted-residual-risks)
identify the owner, rationale, mitigation, and review condition for each
remaining risk.
