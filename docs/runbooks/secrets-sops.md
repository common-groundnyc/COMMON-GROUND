# Secrets (SOPS + age)

All secrets live in `.env.secrets.enc` (committed, encrypted with age). `.env.secrets` is gitignored.

## Edit

```bash
sops --input-type dotenv --output-type dotenv .env.secrets.enc
```

## Decrypt to local `.env.secrets`

```bash
sops --decrypt --input-type dotenv --output-type dotenv .env.secrets.enc > .env.secrets
```

## Rules

- **Never** read `*.secrets.toml` files directly from code or agents.
- **Never** echo secrets to the terminal (`env`, `printenv`, `cat`).
- **Never** commit `.env.secrets` (decrypted).
- Use `dlt.secrets["key"]` in Python when applicable.
