# Contributing to VIBE-CODE

Thanks for your interest in contributing. This document explains how to get involved.

## Getting Started

1. Fork the repository
2. Clone your fork locally
3. Create a branch from `main` for your work
4. Make your changes
5. Push to your fork and submit a pull request

## Development Setup

```bash
git clone https://github.com/kspavankrishna/VIBE-CODE.git
cd VIBE-CODE
# Install dependencies as described in README.md
```

## Branch Naming

Use descriptive branch names following this convention:

- `feat/short-description` for new features
- `fix/short-description` for bug fixes
- `docs/short-description` for documentation changes
- `refactor/short-description` for code refactoring
- `test/short-description` for test additions or changes

## Commit Messages

Write clear commit messages. Use the format:

```
type: concise description of what changed

Optional longer explanation of why the change was made,
what problem it solves, and any context a reviewer needs.
```

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `ci`, `perf`

## Pull Request Process

1. Update documentation if your change affects public interfaces or behavior
2. Add or update tests for any new functionality
3. Ensure all existing tests pass before submitting
4. Fill out the pull request template completely
5. Link any related issues using GitHub keywords (e.g., "Closes #42")
6. Request review from at least one maintainer

PRs require at least one approving review before merge. Maintainers may request changes or ask questions before approving.

## Code Style

- Follow the existing patterns in the codebase
- Keep functions focused and reasonably sized
- Comment complex logic but prefer self-documenting code
- No commented-out code in commits

## Reporting Bugs

Use the Bug Report issue template. Include:

- Clear steps to reproduce
- Expected behavior vs actual behavior
- Environment details (OS, runtime version, etc.)
- Logs or error output if applicable

## Requesting Features

Use the Feature Request issue template. Describe:

- The problem you are trying to solve
- Your proposed solution
- Alternatives you have considered

## Security Vulnerabilities

Do **not** open a public issue for security vulnerabilities. Follow the process described in [SECURITY.md](SECURITY.md).

## Code of Conduct

All contributors are expected to follow our [Code of Conduct](CODE_OF_CONDUCT.md). Be respectful, constructive, and collaborative.

## Questions

Open a Discussion on the repository if you have questions about contributing. I am happy to help.
