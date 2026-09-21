# Migrations

Alembic owns the schema. The models in `tradewatch.infrastructure.database.models` describe
what the schema *should* be; the files in `versions/` are how any database gets there.

The database URL comes from `TW_DATABASE_URL` through the typed settings, never from
`alembic.ini` — one source of truth for where the database is, and no credential in a
committed file.

## Commands

Run from `backend/`:

```bash
uv run alembic upgrade head              # apply everything
uv run alembic downgrade -1              # undo the last migration
uv run alembic current                   # what is applied
uv run alembic history --verbose         # the full history
uv run alembic upgrade head --sql        # emit SQL instead of running it
uv run alembic revision --autogenerate -m "add something"
```

## Rules

1. **Autogenerate drafts, humans review.** Alembic is good at spotting added tables and
   columns. It does not see a rename — it sees a drop and an add, which on a populated
   table means silent data loss. Read every generated migration before committing it.

2. **Every migration must downgrade.** CI runs `upgrade → downgrade → upgrade` and a
   `downgrade` body of `pass` fails that. If a migration genuinely cannot be reversed, say
   so in the docstring and explain what recovery looks like.

3. **Expand and contract for anything with data behind it.** Add the new column nullable,
   backfill, make it NOT NULL, then remove the old one — across separate migrations, so
   old and new application code both work while a deploy is in flight (failure F20).

4. **A migration never imports application code.** It must still run in five years when
   the model it was generated from has changed beyond recognition. Copy the table
   definition into the migration rather than importing it.

5. **No data migration and schema migration in one file.** They need different review, and
   they fail differently.

## Tests

`tests/integration/test_migrations.py` asserts that migrations build the schema from empty,
that they reverse, and — the one that matters most — that autogenerate finds **no
difference** between the models and a freshly migrated database. That last test is what
stops the models and the migrations from drifting apart, which is otherwise discovered in
production.
