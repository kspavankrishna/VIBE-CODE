# Security Policy

## Supported Versions

| Version | Supported          | End of Support |
| ------- | ------------------ | -------------- |
| 5.1.x   | ✅ Active           | Current        |
| 5.0.x   | ⚠️ Critical only    | 2026-12-31     |
| 4.0.x   | ⚠️ Critical only    | 2026-06-30     |
| < 4.0   | ❌ Unsupported      | —              |

Unsupported versions receive no patches. Upgrade immediately.

## Reporting a Vulnerability

**Do not open a public issue for security vulnerabilities.**

Report privately through one of these channels:

1. **GitHub Security Advisories** (preferred): Navigate to the Security tab of this repository and select "Report a vulnerability"
2. **Email**: kspavankrishna@gmail.com

### What to include

- Affected component(s) and version(s)
- Step-by-step reproduction instructions
- Proof of concept (code, screenshots, logs) if available
- Impact assessment: what an attacker could achieve
- Any suggested fix or mitigation

### Response commitment

| Stage | Timeframe |
| ----- | --------- |
| Acknowledgment of report | 24 hours |
| Initial triage and severity classification | 72 hours |
| Status update to reporter | 7 calendar days |
| Patch release (Critical/High) | 14 calendar days |
| Patch release (Medium/Low) | 30 calendar days |
| Public disclosure | After patch is released or 90 days from report, whichever comes first |

### Severity classification

We follow CVSS v3.1 scoring:

| Rating | CVSS Score | Description |
| ------ | ---------- | ----------- |
| Critical | 9.0 – 10.0 | Remote code execution, authentication bypass, data exfiltration with no user interaction |
| High | 7.0 – 8.9 | Privilege escalation, significant data exposure, denial of service |
| Medium | 4.0 – 6.9 | Limited data exposure, requires authentication or user interaction |
| Low | 0.1 – 3.9 | Informational, minimal impact, defense-in-depth hardening |

### What happens after you report

1. We confirm receipt within 24 hours
2. A maintainer reproduces and validates the issue
3. We assign a CVE identifier if applicable
4. We develop and internally test a fix
5. We release the patch and publish a security advisory
6. We credit you in the advisory (unless you prefer anonymity)

## Responsible Disclosure Policy

We ask that reporters:

- Allow us reasonable time to investigate and patch before any public disclosure
- Avoid accessing or modifying other users' data
- Act in good faith to avoid degradation of service
- Not exploit a vulnerability beyond what is necessary to demonstrate it

We commit to:

- Not pursuing legal action against researchers acting in good faith
- Working with you to understand and resolve the issue
- Crediting you publicly (with your consent) for your discovery

## Security Practices

### Code integrity
- All commits to `main` require signed commits and pull request review
- Dependencies are monitored via Dependabot and scanned on every PR
- CI pipeline runs SAST (static analysis) on every push

### Token and secret management
- No secrets, API keys, or credentials are stored in source code
- All sensitive configuration uses environment variables or a secrets manager
- `.gitignore` and pre-commit hooks prevent accidental secret commits

### Supply chain
- Third-party dependencies are pinned to exact versions
- Dependency updates are reviewed manually before merge
- Lock files are committed and integrity-checked in CI

## Security-Related Configuration

If you deploy VIBE-CODE in production, we recommend:

- Enable TLS for all network communication
- Restrict file system permissions to least privilege
- Run processes as a non-root user
- Set resource limits (memory, CPU, file descriptors) to prevent abuse
- Enable audit logging for all administrative actions
- Rotate credentials and tokens on a regular schedule

## Contact

For security matters: kspavankrishna@gmail.com
For general questions: Open a Discussion on this repository

---

This policy is reviewed and updated quarterly. Last reviewed: April 2026.
