# Contributing to EventBase

Thank you for your interest in contributing to `event_base`! This document outlines our development process, coding standards, and expectations for contributors.

## 📋 Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Code Style](#code-style)
- [Commit Guidelines](#commit-guidelines)
- [Pull Request Process](#pull-request-process)
- [AI-Assisted Contributions](#ai-assisted-contributions)
- [Testing](#testing)
- [Documentation](#documentation)
- [License](#license)

---

## Code of Conduct

We are committed to providing a welcoming and inclusive environment. All contributors are expected to adhere to our [Code of Conduct](CODE_OF_CONDUCT.md).

---

## Getting Started

### Prerequisites

- Rust 1.75 or later
- Cargo
- Git

### Building the project

```bash
# Clone the repository
git clone https://github.com/event-base/event_base.git
cd event_base

# Build all crates
cargo build --workspace

# Run tests
cargo test --workspace
```

---

## Development Workflow

1. **Fork the repository** and create your branch from `main`.
2. **Make your changes** with clear, focused commits.
3. **Write or update tests** for your changes.
4. **Run the full test suite** before submitting.
5. **Open a Pull Request** with a clear description of your changes.

### Branch naming

Use descriptive branch names:

- `feature/describe-your-feature`
- `fix/describe-the-bug`
- `docs/describe-the-documentation`
- `refactor/describe-the-refactor`

---

## Code Style

We enforce a consistent code style using `rustfmt` and `clippy`.

```bash
# Format code
cargo fmt

# Check with clippy
cargo clippy -- -D warnings
```

### Style guidelines

- **Use `rustfmt` defaults.** No exceptions.
- **Write clear, self-documenting code.** Comments should explain *why*, not *what*.
- **Avoid `unwrap()` and `expect()`** in library code. Use proper error handling with `?`.
- **Document public APIs** with `///` doc comments.
- **Keep functions small and focused.** Single responsibility principle.

---

## Commit Guidelines

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types

| Type       | Description                           |
|------------|---------------------------------------|
| `feat`     | New feature                           |
| `fix`      | Bug fix                               |
| `docs`     | Documentation changes                 |
| `style`    | Code style (formatting, etc.)         |
| `refactor` | Code refactoring (no behavior change) |
| `perf`     | Performance improvements              |
| `test`     | Adding or updating tests              |
| `chore`    | Build process or tooling changes      |
| `ci`       | CI configuration changes              |

### Examples

```
feat(core): add topic discovery mechanism

- Add `_system.topic_discovery` system topic
- Implement request/response for topic sync
- Update WorkerRegistry to store topic metadata

Closes #42
```

```
fix(wal): ensure flush is called on append

Previously, data was only written to memory and not persisted.
This fixes data loss on application restart.
```

---

## Pull Request Process

### Before submitting

1. **Update the documentation** for any public API changes.
2. **Add tests** for new functionality.
3. **Ensure all tests pass** locally.
4. **Run `cargo fmt` and `cargo clippy`**.

### PR Requirements

- **Clear title and description** explaining the change.
- **Link to any related issues**.
- **Pass all CI checks**.
- **Minimum one approval** from a maintainer.

### Review Process

1. A maintainer will review your PR within 1-3 days.
2. Address any feedback and push changes to your branch.
3. Once approved, a maintainer will squash and merge.

---

## AI-Assisted Contributions

We welcome the use of AI tools (such as GitHub Copilot, Cursor, or LLMs) to assist in development. However, **AI-generated code must be reviewed with extra scrutiny**.

### Guidelines

1. **No "vibe coding" submissions.** Vibe coding refers to accepting AI-generated code without understanding what it does. Every line of code must be understood and justified by the author.

2. **You are responsible for the code.** AI is a tool, not a co-author. You are accountable for correctness, performance, and security.

3. **Test AI-generated code thoroughly.** AI often produces code that looks correct but has subtle bugs. Write tests that cover edge cases.

4. **Explain AI-generated changes.** In your PR description, mention which parts were AI-assisted and explain why the solution is correct.

5. **AI-generated code must still be reviewed.** No special treatment. Maintainers will review AI-generated code with the same rigor as human-written code.

6. **Do not submit PRs that are 100% AI-generated** without significant human contribution. The PR must demonstrate thoughtful work.

### Acceptable AI Use

- Autocompletion of boilerplate code
- Generating test fixtures
- Refactoring suggestions
- Documentation generation
- Learning and exploration

### Unacceptable AI Use

- Copy-pasting entire AI-generated functions without understanding
- Submitting PRs with code you cannot explain
- Using AI to bypass proper testing or design work
- Large-scale AI-generated changes without clear human oversight

---

## Testing

We require comprehensive testing for all contributions.

### Types of tests

| Test Type             | Purpose                        | Location                    |
|-----------------------|--------------------------------|-----------------------------|
| **Unit tests**        | Test individual functions      | `#[cfg(test)]` in each file |
| **Integration tests** | Test cross-module behavior     | `event_base_tests/tests/`   |
| **Doc tests**         | Test examples in documentation | Doc comments with `///`     |

### Running tests

```bash
# Run all tests
cargo test --workspace

# Run with logging output
RUST_LOG=debug cargo test --workspace -- --nocapture

# Run a specific test
cargo test test_broadcast_works -- --nocapture
```

### Test coverage

- New features should include tests covering success and failure paths.
- Bug fixes should include tests that would have caught the bug.
- Test coverage should not decrease.

---

## Documentation

Documentation is a first-class citizen in `event_base`.

- **API documentation**: All public APIs must have `///` doc comments.
- **Examples**: Provide example code in doc comments where appropriate.
- **Error messages**: Include `# Errors` sections describing error cases.
- **Panics**: Include `# Panics` sections if the function can panic.

### Example doc comment

```rust
/// Sends a message to the specified topic.
///
/// # Arguments
/// * `topic` - The topic to send to
/// * `msg` - The message to send
///
/// # Errors
/// Returns `CoreError::TopicNotFound` if the topic is not registered.
/// Returns `CoreError::QueueFull` if the queue is at capacity.
///
/// # Example
/// ```
/// let msg = EMessage::new("order", b"hello".to_vec());
/// TopicRouter::global().send("order", msg).await?;
/// ```
pub async fn send(topic: &str, msg: EMessage) -> Result<(), CoreError> {
    // ...
}
```

---

## License

By contributing to `event_base`, you agree that your contributions will be licensed under the project's [LICENSE](LICENSE) file.

---

## Questions?

If you have questions about contributing, feel free to:

- Open a [GitHub Discussion](https://github.com/RedElectricity/event_base/discussions)
- Email me at [my email](mailto:redelectricity@outlook.com)

We're excited to see what you'll build with `event_base`! 🚀