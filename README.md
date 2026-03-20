# NL-SQL — Natural Language to SQL Query Engine

A modular, fully swappable NL→SQL system supporting MySQL and MongoDB.
Every component (LLM, embedder, DB adapter, strategy, vector store) is
swappable via a single `.env` change — no code modifications needed.

---

## Project structure

```
nlsql/
├── backend/
│   ├── core/
│   │   ├── interfaces/     ← abstract base classes (owned by lead)
│   │   ├── factory/        ← one factory per swappable concern
│   │   └── config/         ← settings, env loading
│   ├── llm/                ← LLM implementations (owned by lead)
│   ├── rag/                ← schema retrieval + prompt building (owned by lead)
│   │   └── embedders/      ← embedder implementations
│   ├── adapters/           ← DB adapters — MySQL + Mongo (Dev 1)
│   │   └── schema_inspector/
│   ├── strategies/         ← query strategies (Dev 2)
│   ├── api/                ← FastAPI routes + middleware (Dev 3)
│   │   ├── routes/
│   │   └── middleware/
│   └── tests/
│       ├── unit/
│       └── integration/
└── frontend/               ← React + Vite (Dev 3)
    └── src/
        ├── components/
        ├── pages/
        ├── services/
        ├── hooks/
        └── types/
```

---

## Quickstart

```bash
# Backend
cd backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # fill in your values
uvicorn api.main:app --reload

# Frontend
cd frontend
npm install
npm run dev
```

---

## Swapping components

Edit `.env` — no code changes needed:

```env
LLM_PROVIDER=openai          # → gemini | ollama
EMBEDDER_PROVIDER=openai     # → cohere
DB_TYPE=mysql                # → mongo
STRATEGY=auto                # → sql | fuzzy | vector | combined
VECTOR_STORE=faiss           # → pinecone
```

---

## Team + branch ownership

| Branch | Owner | Folder |
|---|---|---|
| `main` | Lead | merge only via PR |
| `feat/db-adapters` | Dev 1 | `backend/adapters/` |
| `feat/strategies` | Dev 2 | `backend/strategies/` |
| `feat/api-ui` | Dev 3 | `backend/api/` + `frontend/` |

Lead works directly on `main` for `core/`, `llm/`, `rag/`.

---

## PR rules

- All PRs must target `main`
- At least 1 review (lead) before merge
- No direct push to `main` (branch protection on)
- Each PR should reference the task from `TASKS.md`
