"""
Lead owns this file.
Builds the final prompt sent to the LLM using the retrieved schema context.
 
UPDATED: Integrates the two-stage system prompts (SQL Generator + NL Response Generator)
and the classicmodels DB context for high-quality, business-aware output.
"""
from core.interfaces import BaseDBAdapter
 
 
# ══════════════════════════════════════════════════════════════════════════════
# SYSTEM PROMPTS (injected as `system` role in LLM API calls)
# These are the authoritative instruction layers for each LLM call.
# They are NEVER shown to the user — they shape how the model behaves.
# ══════════════════════════════════════════════════════════════════════════════
 
# ── SYSTEM PROMPT 1: SQL Generation ──────────────────────────────────────────
# Injected as the `system` message in the first LLM call (NL → SQL).
# Tells the model exactly how to behave, what the business does, and what rules to follow.
SQL_SYSTEM_PROMPT = """
You are the SQL generation engine for an internal business analytics chatbot built on the `classicmodels` database. Your one and only job is to convert a natural language question into a single, correct, executable MySQL SQL query — nothing more.

You do not explain the query. You do not greet the user. You do not ask clarifying questions unless the question is genuinely ambiguous and cannot be safely resolved. You output SQL only.

If the question cannot be answered from this database, output:
__OUT_OF_SCOPE__
REASON: <one sentence explaining what data is missing>
SUGGEST: <one concrete answerable rephrasing using actual table/column names from this DB>

If clarification is needed, output:
__CLARIFY__
AMBIGUITY: <exactly what is unclear>
OPTIONS: <2-3 bullet interpretations using actual schema terms>

If the user asks for card numbers, CVV, or card expiry data, output:
__PRIVACY_BLOCK__
REASON: card_number, cvv, and card_expiry are hard-blocked sensitive columns.

---

## BUSINESS CONTEXT

`classicmodels` is a B2B wholesale distributor of scale model vehicles — classic cars, motorcycles, planes, ships, trains, trucks, and vintage cars.

- Customers are businesses. `customerName` is a company name. `contactFirstName`/`contactLastName` is the human point of contact.
- Sales reps (`salesRepEmployeeNumber`) manage customer accounts. Reps report to managers via `employees.reportsTo` (self-FK).
- Offices are operational hubs — they have a `city`, `country`, and `territory`. They are NOT a sales dimension on their own. Never GROUP BY or filter on `offices` alone when the question is about revenue, customers, or performance — always go through employees → customers → orders.
- Revenue is NEVER stored on `orders`. Always compute: SUM(od.quantityOrdered * od.priceEach) from `orderdetails`.
- Payments are customer-level settlements, NOT linked to specific orders. Never JOIN `payments` to `orderdetails`.
- Three price points: `buyPrice` (cost), `MSRP` (list price), `orderdetails.priceEach` (actual sale price).
- Payment methods: 'UPI', 'NEFT', 'CHECK' — stored on both `customers` and `payments`.
- Order status: 'Shipped', 'In Process', 'On Hold', 'Cancelled', 'Disputed', 'Resolved'.

---

## SQL RULES

Correctness:
1. Always use fully qualified references when joining: o.customerNumber, not just customerNumber.
2. Revenue = SUM(od.quantityOrdered * od.priceEach) from orderdetails od — never from orders.
3. Use safe_customers and safe_payments views by default.
4. Manager/team hierarchy = self-join or recursive CTE on employees.reportsTo.
5. Nullable columns (shippedDate, state, salesRepEmployeeNumber, reportsTo) — use IS NULL / IS NOT NULL.
6. Date arithmetic: use YEAR(), MONTH(), CURDATE(), DATEDIFF() — never hardcode dates unless user specifies.
7. String matching: use LOWER() or LIKE unless exact match is clearly intended.
8. Default LIMIT 50 unless question asks for all records or an aggregate.
9. CRITICAL — offices bias fix: questions about territory performance must route through offices → employees → customers → orders → orderdetails. Never report office-level metrics directly from the offices table alone.

Style:
10. Standard MySQL only. WITH RECURSIVE only when recursion is required.
11. Aliases: c=customers, e=employees, o=orders, od=orderdetails, p=products, pl=productlines, pay=payments, off=offices.
12. Column aliases in human-readable snake_case: total_revenue, order_count, avg_order_value.
13. Always ORDER BY on list queries, default DESC on the primary metric.
14. No trailing semicolon. Single query only.

Privacy:
15. NEVER SELECT cvv, card_number, card_expiry — output __PRIVACY_BLOCK__ instead.

Silent ambiguity resolution:
- "best customers" → highest SUM(amount) from payments
- "top products" → highest total quantityOrdered
- "sales performance" → SUM(od.quantityOrdered * od.priceEach) grouped by salesRepEmployeeNumber
- "recent orders" → ORDER BY orderDate DESC LIMIT 10
- "revenue" → SUM(od.quantityOrdered * od.priceEach)
- "profit" → SUM((od.priceEach - p.buyPrice) * od.quantityOrdered)
- "active customers" → customers with at least one order status NOT 'Cancelled'
- "late orders" → shippedDate > requiredDate OR (shippedDate IS NULL AND requiredDate < CURDATE())
- "by region" or "by territory" → JOIN through offices.territory via employees
""".strip()

# ── SYSTEM PROMPT 2: NL Answer Generation ────────────────────────────────────
# Injected as the `system` message in the second LLM call (result → NL answer).
# Tells the model how to format, interpret, and present query results.
ANSWER_SYSTEM_PROMPT = """
You are the ClassicModels Analytics Assistant — an internal business chatbot for Classic Models, a B2B wholesale distributor of scale model vehicles.

You receive the user's original question, the SQL query executed, and the raw results. Your job is to answer clearly, honestly, and only from what the data contains. Never fabricate, extrapolate, or guess.

---

## WHEN ASKED WHAT YOU CAN DO / WHAT DATA IS AVAILABLE

Do NOT list table names, column counts, or row counts. Instead respond with a capability overview like:

"I can help you explore the Classic Models business across these areas:
- **Sales & Revenue** — total revenue, revenue by product line, by sales rep, by territory, or by time period
- **Customers** — top customers by spend, customers by region, unassigned accounts, payment methods
- **Products** — best-selling models, inventory levels, profit margins, price vs cost comparison
- **Orders** — order status breakdown, overdue/late orders, order history for a customer
- **Employees & Teams** — sales rep performance, manager's team, office locations
- **Payments** — payment totals by customer, payment method breakdown

Just ask in plain English — for example: 'Who are the top 5 customers by revenue this year?' or 'Which product line has the highest profit margin?'"

---

## RESPONSE RULES

1. Answer the question directly first. Lead with the business insight.
2. Be precise — mention actual values from the results.
3. Use business language, not database language:
   - customerNumber → customer ID or company name
   - salesRepEmployeeNumber → sales rep / account manager
   - priceEach → sale price, buyPrice → cost price, MSRP → list price
   - orderNumber → order reference, quantityInStock → inventory
   - territory → sales region, productLine → product category
4. Format results correctly:
   - Single value: one sentence
   - If results contain tabular data (rows with columns): ALWAYS render as a markdown table, max 10 rows.
     - After the table, if total rows > 10, add exactly: "📄 Showing 10 of {row_count} rows. Reply **'show more'** to see the next 10."
     - If total rows ≤ 10, render all rows in the table.
   - Currency: always $X,XXX.XX format
   - Dates: Jan 6, 2003
   - Percentages: round to 1 decimal place
   - Large numbers: comma separators
5. SIGNAL handling:
   - __EMPTY_RESULT__: explain in business terms what was not found, offer a rephrased suggestion
   - __OUT_OF_SCOPE__: decline firmly, redirect to one related thing you CAN answer
   - __PRIVACY_BLOCK__: decline firmly, offer payment totals or method breakdown instead
   - __SQL_ERROR__: apologize briefly, do not expose the raw error, suggest rephrasing
   - __CLARIFY__: present options in plain English, ask which the user meant
6. Sparse results (1-2 rows for a broad question): note this and suggest broadening.
7. Never fabricate data not in the result.
8. Keep it concise: 1-3 sentences for lookups, list + summary for rankings.
9. No "Great question!", no pleasantries, no AI self-references.
10. NEVER answer using general world knowledge — only what this database can show.

---

## REPHRASED QUERY SUGGESTION (ALWAYS APPLY)

After every answer, add a single line:

Try asking: "<a rephrased version of the user's question that uses the correct schema terms and will produce a more accurate or richer result>"

Rules for the rephrased suggestion:
- Use actual column/table concepts the user may not know: e.g. "by territory" instead of "by country", "sales rep" instead of "manager", "product line" instead of "category"
- Make it a natural English question, not SQL
- Only suggest something meaningfully different or more precise than what was asked
- If the original question was already perfectly precise, skip this line
- Never suggest something outside the DB scope
""".strip()
 
 
# ══════════════════════════════════════════════════════════════════════════════
# DB SCHEMA CONTEXT (injected into the USER message of the SQL generation call)
# This is the classicmodels_nlsql_context.md content, condensed for LLM injection.
# It supplements the RAG-retrieved schema chunks with business rules and query patterns.
# ══════════════════════════════════════════════════════════════════════════════
 
DB_SCHEMA_CONTEXT = """
## DATABASE: classicmodels
 
### Tables
| Table | PK | Purpose |
|-------|----|---------|
| customers | customerNumber | B2B customer master — company identity, address, assigned rep, payment method |
| employees | employeeNumber | Staff + org hierarchy (reportsTo self-FK) |
| offices | officeCode | 7 physical offices; territory = 'NA','EMEA','APAC','Japan' |
| orders | orderNumber | Order headers — dates, status, customerNumber |
| orderdetails | (orderNumber, productCode) | Order line items — quantityOrdered, priceEach |
| payments | (customerNumber, checkNumber) | Customer-level payments — not linked to specific orders |
| products | productCode | SKU catalogue — buyPrice (cost), MSRP (list), quantityInStock |
| productlines | productLine | 7 categories: Classic Cars, Motorcycles, Planes, Ships, Trains, Trucks and Buses, Vintage Cars |
 
### Safe Views (use by default)
- safe_customers: customers without cvv, card_number, card_expiry, upi_id, account_number, ifsc_code, creditLimit
- safe_payments: payments without checkNumber, upi_id, account_number, ifsc_code, card_number, cvv, card_expiry
 
### Key Column Notes
- salesRepEmployeeNumber (customers) → FK to employees.employeeNumber; NULL = no rep assigned
- reportsTo (employees) → self-FK; NULL = President (top of org)
- territory (offices) → 'NA', 'EMEA', 'APAC', 'Japan'
- status (orders) → 'Shipped','In Process','On Hold','Cancelled','Disputed','Resolved'
- shippedDate (orders) → NULL means not yet shipped
- payment_method (customers, payments) → 'UPI', 'NEFT', 'CHECK'
- productCode prefix → S10=1:10 scale, S12=1:12, S18=1:18, S24=1:24, S32=1:32, S50=1:50
 
### FK Chain
offices.officeCode → employees.officeCode
employees.employeeNumber (reportsTo self-ref hierarchy)
employees.employeeNumber → customers.salesRepEmployeeNumber
customers.customerNumber → orders.customerNumber → orderdetails.orderNumber
orderdetails.productCode → products.productCode → productlines.productLine
customers.customerNumber → payments.customerNumber
 
### Common Query Patterns
| Goal | How |
|------|-----|
| Revenue for an order | SUM(od.quantityOrdered * od.priceEach) from orderdetails |
| Revenue by customer | JOIN orders + orderdetails, GROUP BY customerNumber |
| Revenue by product line | JOIN orderdetails → products → productlines |
| Sales rep performance | JOIN customers → orders → orderdetails, GROUP BY salesRepEmployeeNumber |
| Customers by territory | JOIN customers → employees → offices, filter on offices.territory |
| Overdue orders | orders WHERE shippedDate IS NULL AND requiredDate < CURDATE() |
| Customers with no orders | LEFT JOIN customers → orders, WHERE orders.orderNumber IS NULL |
| Profit margin | (orderdetails.priceEach - products.buyPrice) * quantityOrdered |
| Payment method breakdown | GROUP BY payment_method on safe_payments |
| Manager's direct reports | Self-join: employees e1 JOIN employees e2 ON e2.reportsTo = e1.employeeNumber |
""".strip()
 
 
# ══════════════════════════════════════════════════════════════════════════════
# USER-MESSAGE TEMPLATES (the actual question + context sent as the `user` role)
# The system prompts above are injected separately as the `system` role.
# ══════════════════════════════════════════════════════════════════════════════
 
# Template for the user turn of the SQL generation call.
# NOTE: DB_SCHEMA_CONTEXT is prepended once here as "persistent" schema knowledge.
# The RAG-retrieved schema_chunks add table-specific detail on top of this.
SQL_USER_TEMPLATE = """
## Static DB Context
{db_schema_context}
 
## RAG-Retrieved Schema (most relevant tables for this question)
{schema_context}
 
## Previous Attempts (if any)
{attempt_history}
 
## Question
{question}
""".strip()
 
 
MONGO_USER_TEMPLATE = """
## RAG-Retrieved Schema (inferred from sampled documents)
{schema_context}
 
## Previous Attempts (if any)
{attempt_history}
 
## Question
{question}
""".strip()
 
 
# Template for the user turn of the NL answer generation call.
ANSWER_USER_TEMPLATE = """
{context}User question: {question}
 
Query executed:
{sql_query}
 
Results ({row_count} row(s) returned):
{results_preview}
 
{quality_instruction}
""".strip()
 
 
# ── Quality instructions injected into the answer user message ────────────────
_QUALITY_INSTRUCTIONS: dict[str, str] = {
    "ok": (
        "Write a clear, concise natural language answer based on the data above. "
        "Be specific — mention actual values from the results."
    ),
    "empty": (
        "No results were returned. Tell the user clearly that nothing was found. "
        "Suggest one or two likely reasons: a filter value that doesn't match the data, "
        "no records for that time period, or a possible typo in a name. "
        "Offer a concrete rephrased question they could try next."
    ),
    "all_null": (
        "The query returned rows but all values appear to be empty or null. "
        "Tell the user the query ran successfully but the data in those fields "
        "appears to be missing or not yet populated. "
        "Suggest they check whether the data exists or try querying a different table."
    ),
    "low_relevance": (
        "These results may not directly answer the question — the returned columns "
        "do not closely match what was asked. Be honest about this: briefly describe "
        "what was actually returned, explain why it might not be the right data, "
        "and suggest how the user could rephrase their question to get a better result. "
        "Do not fabricate an answer from unrelated data."
    ),
}
 
 
# ══════════════════════════════════════════════════════════════════════════════
# PromptBuilder
# ══════════════════════════════════════════════════════════════════════════════
 
class PromptBuilder:
    def __init__(self, adapter: BaseDBAdapter):
        self.adapter = adapter
 
    # ── SQL / Query generation ────────────────────────────────────────────────
 
    def build_query_prompt(
        self,
        question: str,
        schema_chunks: list[dict],
        attempt_history: list = None,
    ) -> dict[str, str]:
        """
        Build the SQL/Mongo generation prompt.
 
        Returns a dict with 'system' and 'user' keys so the caller can pass
        each to the correct role in the LLM API call:
 
            llm.generate(system=prompt["system"], user_message=prompt["user"])
 
        The system prompt encodes all behavioural rules and business context.
        The user message supplies the dynamic schema chunks + question.
        """
        schema_context = "\n".join(
            chunk["schema_text"] for chunk in schema_chunks
        )
        history_text = self._format_history(attempt_history or [])
 
        if self.adapter.db_type == "mysql":
            user_content = SQL_USER_TEMPLATE.format(
                db_schema_context=DB_SCHEMA_CONTEXT,
                schema_context=schema_context,
                attempt_history=history_text,
                question=question,
            )
            return {
                "system": SQL_SYSTEM_PROMPT,
                "user": user_content,
            }
 
        # MongoDB path — no static DB context (schema is inferred at runtime)
        user_content = MONGO_USER_TEMPLATE.format(
            schema_context=schema_context,
            attempt_history=history_text,
            question=question,
        )
        return {
            "system": SQL_SYSTEM_PROMPT,   # same safety rules apply
            "user": user_content,
        }
 
    # ── NL answer generation ──────────────────────────────────────────────────
 
    def build_answer_prompt(
        self,
        question: str,
        rows: list[dict],
        row_count: int,
        quality: str = "ok",
        sql_query: str = "",
        context: list[dict] | None = None,
    ) -> dict[str, str]:
        """
        Build the natural language answer generation prompt.
 
        Returns a dict with 'system' and 'user' keys, same pattern as
        build_query_prompt. The caller passes each to the correct role.
 
        Args:
            question:   The original user question.
            rows:       Full result rows from the DB (may be large).
            row_count:  Total number of rows.
            quality:    Result quality signal — 'ok' | 'empty' | 'all_null' | 'low_relevance'
            sql_query:  The actual SQL/Mongo query that was executed.
            context:    Last N conversation turns for sliding window context.
                        Each turn: {question, sql, answer}
        """
        from core.config.settings import get_settings
        max_for_llm = get_settings().MAX_ROWS_FOR_LLM
 
        preview_rows = rows[:max_for_llm]
 
        # Strip embedding columns — they are huge vectors, not human-readable.
        cleaned = [
            {k: v for k, v in row.items() if "embed" not in k.lower()}
            for row in preview_rows
        ]
        results_preview = "\n".join(str(row) for row in cleaned)
 
        if row_count > max_for_llm:
            results_preview += (
                f"\n\n(Note: showing {max_for_llm} of {row_count} total rows. "
                f"The user can see all {row_count} rows in the results table.)"
            )
 
        quality_instruction = _QUALITY_INSTRUCTIONS.get(
            quality, _QUALITY_INSTRUCTIONS["ok"]
        )
 
        # Build sliding window context block (hard cap at 5 turns)
        context_text = ""
        if context:
            lines = ["Previous conversation turns (for context):"]
            for i, turn in enumerate(context[-5:], 1):
                lines.append(f"  Turn {i}:")
                lines.append(f"    Q: {turn.get('question', '')}")
                lines.append(f"    SQL: {turn.get('sql', '')}")
                lines.append(f"    A: {turn.get('answer', '')}")
            context_text = "\n".join(lines) + "\n\n"
 
        user_content = ANSWER_USER_TEMPLATE.format(
            context=context_text,
            question=question,
            sql_query=sql_query or "(not available)",
            row_count=row_count,
            results_preview=results_preview or "(no rows returned)",
            quality_instruction=quality_instruction,
        )
 
        return {
            "system": ANSWER_SYSTEM_PROMPT,
            "user": user_content,
        }
 
    def _format_history(self, history: list) -> str:
        if not history:
            return "None"
        lines = []
        for attempt in history:
            lines.append(
                f"Attempt {attempt.attempt_number}: {attempt.query_used}\n"
                f"Error: {attempt.error}"
            )
        return "\n\n".join(lines)
 