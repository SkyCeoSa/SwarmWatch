# Contributing to SwarmWatch

Thank you for your interest in contributing to **SwarmWatch**! 👁️

SwarmWatch is the universal observability, zero-token entropy circuit-breaker, and time-machine debugging platform for multi-agent systems.

---

## 🏛️ Development Philosophy

We follow strict software engineering best practices inspired by Matt Pocock:
1. **Vertical Slicing**: Every PR must represent a cohesive, usable vertical slice with end-to-end functionality.
2. **Test-Driven Development (TDD)**: Write the failing test first, verify failure, then write minimal clean code to make it pass.
3. **Deep Modules**: Strive for simple, developer-friendly interfaces that conceal internal complexity (sub-microsecond queuing, WAL pragmas, Shannon math).
4. **Zero Framework Churn**: Avoid monkey-patching third-party libraries. Use official callbacks, hooks, and clean composition.

---

## 🛠️ Local Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/SkyCeoSa/SwarmWatch.git
   cd SwarmWatch
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Or on Windows: .venv\Scripts\activate
   ```

3. **Install dependencies in editable mode:**
   ```bash
   pip install -e packages/sdk-python
   pip install pytest pytest-asyncio httpx websockets fastapi uvicorn
   ```

4. **Run the test suite:**
   ```bash
   pytest packages/sdk-python/tests/ -v
   ```

---

## 🧪 Testing Guidelines

- All new features must include unit or integration tests in `packages/sdk-python/tests/`.
- Ensure tests execute deterministically in sub-seconds.
- No network I/O in unit tests; use SQLite in-memory or temporary files.

---

## 📬 Submitting a Pull Request

1. Create a descriptive feature branch: `git checkout -b feat/my-new-feature`.
2. Commit your changes using conventional commits: `feat: add LangGraph streaming support`.
3. Push to your fork and submit a PR to `main`.
4. GitHub Actions CI will automatically test your PR against Python 3.10, 3.11, and 3.12.
