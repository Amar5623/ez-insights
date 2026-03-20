# Contributing guide

> Read this before writing a single line of code.

---

## Setup

```bash
git clone <repo-url>
cd nlsql

# Backend
cd backend
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
cp core/config/.env.example .env
# Edit .env with your actual keys

# Frontend (separate terminal)
cd frontend
npm install
cp .env.example .env.local
# Edit .env.local — set VITE_API_URL=http://localhost:8000
```

---

## The one rule that matters most

**Stay in your folder. Use the interface. Never import across boundaries.**

| You are | You work in | You import from |
|---|---|---|
| Dev 1 | `backend/adapters/` | `core/interfaces/` only |
| Dev 2 | `backend/strategies/` | `core/interfaces/` only |
| Dev 3 | `backend/api/` + `frontend/` | `api/schemas.py`, `services/query_service.py` |

If you need something from another module, ask the lead to expose it via an interface.
**Never do:** `from adapters.mysql_adapter import MySQLAdapter` inside `strategies/`.
**Always do:** `from core.interfaces import BaseDBAdapter` and use what's injected.

---

## Git workflow

### Your branch

```bash
# Dev 1
git checkout -b feat/db-adapters

# Dev 2
git checkout -b feat/strategies

# Dev 3
git checkout -b feat/api-ui
```

### Daily flow

```bash
git pull origin main             # sync with latest main before starting work
# ... make your changes ...
git add .
git commit -m "feat: implement MySQLAdapter.execute_query"
git push origin feat/db-adapters
# Open PR on GitHub → target: main
```

### Commit message format

```
feat: short description of what you added
fix: short description of what you fixed
test: added or updated tests
refactor: restructured without changing behaviour
```

### Never

- Push directly to `main`
- Commit `.env` or `.env.local`
- Import across module boundaries (see rule above)
- Leave `raise NotImplementedError` in a function you're supposed to complete

---

## Running tests

```bash
cd backend

# Run your own unit tests
pytest tests/unit/test_mysql_adapter.py -v    # Dev 1
pytest tests/unit/test_sql_validator.py -v    # Dev 2
pytest tests/unit/test_router.py -v           # Dev 2
pytest tests/unit/test_retry_handler.py -v   # Dev 2

# Run all unit tests
pytest tests/unit/ -v

# Run integration tests (lead runs these after all modules are connected)
pytest tests/integration/ -v
```

All tests must pass before opening a PR. If a test is still marked `pytest.skip`,
that's fine — but do not remove the skip without implementing the feature.

---

## Definition of done (per PR)

Before requesting a review, check every item:

- [ ] All functions in your module are implemented (no `raise NotImplementedError`)
- [ ] Your unit tests pass (`pytest tests/unit/ -v`)
- [ ] No hardcoded secrets, passwords, or connection strings
- [ ] You only touched files in your assigned folder
- [ ] PR description filled out using the template
- [ ] `.env.example` updated if you added new config keys

---

## How the factory system works

You never instantiate your own class. The lead's factory does that.

```python
# This is what happens at app startup (main.py):
adapter = create_db_adapter()    # reads DB_TYPE from .env → returns MySQLAdapter()
strategy = create_strategy(adapter)  # reads STRATEGY → returns SQLFilterStrategy(adapter)
```

To swap MySQL for Mongo:
```env
DB_TYPE=mongo    # .env change only — zero code changes needed
```

Your job is to make sure your class fully implements its interface.
The factory handles the rest.

---

## Asking for help

- Stuck on the interface contract? → Ask the lead, don't guess.
- Need a new env var? → Add it to `core/config/settings.py` AND `.env.example`, then tell the lead.
- Found a bug outside your module? → Open a GitHub issue, don't fix it yourself.
- PR has requested changes? → Address each comment, push a new commit, re-request review.
