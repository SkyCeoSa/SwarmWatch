# 07. Operations & Developer Runbook

This runbook contains exact terminal commands, diagnostic scripts, and verification procedures for building and running SwarmWatch.

---

## 1. Environment Setup

SwarmWatch uses Python 3.12+ with a local virtual environment:

```powershell
# Activate local virtual environment
.venv\Scripts\Activate.ps1

# Verify installed packages
pip list
```

---

## 2. Launching the Live Console & Server

Start the real-time server and open the developer console on port 8765:

```powershell
.venv\Scripts\python.exe -m swarmwatch.cli ui --port 8765
```

- **Dashboard UI**: `http://127.0.0.1:8765/` or `http://127.0.0.1:8765/ui`
- **Health Check**: `http://127.0.0.1:8765/health`
- **WebSocket Endpoint**: `ws://127.0.0.1:8765/ws/swarm/{swarm_id}`
- **Simulation API**: `POST http://127.0.0.1:8765/api/v1/simulate`

---

## 3. Running the Test Suite (TDD)

Execute the full pytest suite across all 16 Python test modules:

```powershell
.venv\Scripts\pytest.exe packages\sdk-python\tests\
```

Expected output:
```
======================= 53 passed, 2 warnings in ~12s =======================
```

Execute the full TypeScript test suite across all 5 test suites:

```powershell
cd packages\sdk-typescript
cmd.exe /c npm test
```

Expected output:
```
# tests 51
# pass 51
# fail 0
```

Total test coverage: **104 automated tests passing (100%)**.

---

## 4. Validating UI Mechanical Quality (Impeccable)

Run the Impeccable mechanical detector on `dashboard.html`:

```bash
python -c "from swarmwatch.acceptance import global_acceptance_harness; print(global_acceptance_harness.audit_dashboard_artifact('dashboard.html'))"
```

Expected output:
```json
[]
```
(Zero anti-pattern findings).

---

## 5. Headless Chrome CDP Verification

To diagnose DOM interactions, WebSocket events, and console exceptions headlessly via Chrome DevTools Protocol:

```python
# Run headless browser diagnostic
.venv\Scripts\python.exe scratch\debug_server_click.py
```
