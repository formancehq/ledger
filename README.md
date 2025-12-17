# Formance Ledger Infrastructure

Deployment infrastructure for Formance Ledger v2 on AWS ECS.

## 🚀 Quick Start

### Local Development

**Note:** This repository is the Formance Ledger source code. To run locally:

```bash
# Start Formance from source (requires Go)
docker compose up

# Or build and run
make up

# Access API at http://localhost:3068
```

### Deploy to Staging
```bash
# Automatic on push to main
git push origin main

# Or manual trigger
# GitHub Actions → deploy-staging → Run workflow
```

### Deploy to Production
```bash
# Manual via GitHub Actions
# Actions → deploy-production → Run workflow
# Input version: v2.3.0
```

## 🏗️ Architecture

- **Service:** formance_ledger
- **Port:** 3068  
- **Image:** ghcr.io/formancehq/ledger:v2.3.0
- **Database:** PostgreSQL (existing RDS)
- **Service Discovery:** formance-ledger.internal.staging-api.tiiik.money
- **ALB Route:** formance.internal.staging-api.tiiik.money

## 🔌 Integration

Your services can call Formance:

```kotlin
// Kotlin services
val formanceUrl = "http://formance-ledger.internal.staging-api.tiiik.money:3068"
httpClient.post("$formanceUrl/v2/transactions") { ... }
```

```typescript
// TypeScript services
const formanceUrl = 'http://formance-ledger.internal.staging-api.tiiik.money:3068';
await axios.post(`${formanceUrl}/v2/transactions`, data);
```

## 📊 Monitoring

- **Logs:** CloudWatch `/ecs/formance-ledger`
- **Metrics:** ECS Container Insights
- **Health:** `GET /_health`

## 🔧 Configuration

**Database credentials are managed automatically by Terraform!**

The connection string flows like this:
1. You set: `export TF_VAR_formance_db_master_password="YourPassword"`
2. Terraform creates RDS and passes connection string to ECS
3. Formance Ledger connects automatically

**No manual configuration needed!** See [DEPLOYMENT_INSTRUCTIONS.md](./DEPLOYMENT_INSTRUCTIONS.md) for details.

## 📚 API Reference

### Create a Ledger
```bash
curl -X POST http://formance.internal.staging-api.tiiik.money/v2 \
  -H "Content-Type: application/json" \
  -d '{"name": "main"}'
```

### Post a Transaction
```bash
curl -X POST http://formance.internal.staging-api.tiiik.money/v2/main/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "postings": [{
      "source": "world",
      "destination": "users:001",
      "amount": "10000",
      "asset": "USD/2"
    }]
  }'
```

### Query Balance
```bash
curl http://formance.internal.staging-api.tiiik.money/v2/main/accounts/users:001/balances
```

## 🗂️ Repository Structure

```
ledger-infra/
├── .github/workflows/
│   ├── staging.yml          # Auto-deploy on push to main
│   └── production.yml        # Manual deploy with version input
├── aws/
│   ├── task-definition-staging.json
│   └── task-definition-prod.json
├── sandbox/
│   ├── docker-compose.yml   # Local Formance + PostgreSQL
│   ├── start.sh
│   ├── stop.sh
│   └── test-api.sh
└── README.md
```

## ⚙️ Prerequisites

### One-Time Setup

1. **Create Database**
   ```sql
   -- Connect to your RDS instance
   CREATE DATABASE formance_ledger;
   CREATE USER formance_user WITH ENCRYPTED PASSWORD 'your_secure_password';
   GRANT ALL PRIVILEGES ON DATABASE formance_ledger TO formance_user;
   ```

2. **Add GitHub Secrets**
   - `FORMANCE_DB_CONN_STRING_STAGING`: `postgresql://formance_user:password@staging-rds.ap-southeast-2.rds.amazonaws.com:5432/formance_ledger?sslmode=require`
   - `FORMANCE_DB_CONN_STRING_PROD`: `postgresql://formance_user:password@prod-rds.ap-southeast-2.rds.amazonaws.com:5432/formance_ledger?sslmode=require`
   - `AWS_ACCESS_KEY_ID_STAGING`
   - `AWS_SECRET_ACCESS_KEY_STAGING`
   - `AWS_ACCESS_KEY_ID_PROD`
   - `AWS_SECRET_ACCESS_KEY_PROD`
   - `AWS_DEFAULT_REGION`: `ap-southeast-2`

3. **Update Task Definitions**
   - Edit `aws/task-definition-staging.json`
   - Edit `aws/task-definition-prod.json`
   - Update the `STORAGE_POSTGRES_CONN_STRING` with actual RDS endpoint

4. **Deploy Infrastructure**
   ```bash
   cd ../tiiik-devops/terraform/staging
   terraform plan
   terraform apply
   ```

## 🚢 Deployment Process

### Staging Deployment

**Triggered by:** Push to `main` branch or manual workflow dispatch

1. Checkout code
2. Configure AWS credentials
3. Render ECS task definition with Formance image
4. Deploy to ECS cluster `tiiik`
5. Wait for service stability

### Production Deployment

**Triggered by:** Manual workflow dispatch with version input

1. Select "deploy-production" workflow in GitHub Actions
2. Click "Run workflow"
3. Enter Formance version (e.g., `v2.3.0`, `v2.4.0`)
4. Confirm deployment

## 🔍 Troubleshooting

### Task won't start
```bash
# Check logs
aws logs tail /ecs/formance-ledger --follow --region ap-southeast-2

# Check task status
aws ecs describe-tasks \
  --cluster tiiik \
  --tasks $(aws ecs list-tasks --cluster tiiik --service formance_ledger --query 'taskArns[0]' --output text) \
  --region ap-southeast-2
```

### Database connection fails
```bash
# Test from bastion/EC2
psql -h your-rds.ap-southeast-2.rds.amazonaws.com -U formance_user -d formance_ledger

# Check security group allows ECS → RDS on port 5432
```

### Health check failing
```bash
# Test health endpoint
curl http://formance.internal.staging-api.tiiik.money/_health

# Should return: {"status":"ok"}
```

## 📖 Documentation

- [Formance Docs](https://docs.formance.com/)
- [API Reference](https://docs.formance.com/api-reference/ledgerv2/list-ledgers)
- [Numscript Language](https://docs.formance.com/modules/ledger/numscript)
- [Example Implementations](https://docs.formance.com/modules/ledger/example-implementations/overview)
