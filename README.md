# 🦅 EagleSense Event Processor

A serverless application that reads DynamoDB events from the past hour, groups them by client, and ships them off to S3. Think of it as a very polite digital mailman that sorts mail into different buckets. 📮

## ⚡ Quick Stats

- **Speed**: 5,734 events/second (faster than your morning coffee order)
- **Scale**: Tested with 200K events per hour (that's a lot of digital paperwork)
- **Success Rate**: 100% (we're perfectionists like that)
- **Memory**: ~200MB peak (lightweight champion)

## 🚀 Getting Started (The Easy Way)

### Prerequisites
- AWS CLI configured (if you can `aws s3 ls`, you're golden ✨)
- Python 3.11+ (because we're not living in 2010)
- Git (obviously)

### Setup (Seriously, Just Two Steps!)

1. **Clone and Setup**
   ```bash
   git clone <repository-url>
   cd processor
   ./scripts/setup_dev_environment.sh
   ```
   *This magical script does everything: venv, dependencies, config files, git hooks, the works!*

2. **Deploy to AWS**
   ```bash
   ./scripts/deploy.sh
   ```
   *Sit back, relax, and watch CloudFormation do its thing. ☕*

That's it! No really, that's actually it. The setup script is like a Swiss Army knife for development environments.

## 🏗️ What's Under the Hood

- **Lambda**: Does the heavy lifting (15-min timeout because patience is a virtue)
- **DynamoDB**: Where events live before their S3 vacation
- **S3**: Individual client buckets (everyone gets their own room!)
- **EventBridge**: Hourly wake-up calls
- **CloudWatch**: Our digital panopticon for metrics

## 🔐 Security (The Boring but Important Stuff)

We follow the "least privilege" principle religiously. Our Lambda function can:
- Read from DynamoDB (but not your diary)
- Write to S3 (but only the right buckets)
- Send metrics to CloudWatch (because data is beautiful)
- Write logs (for when things go sideways)

The CloudFormation template includes all the IAM policies. No manual clicking required! 🎉

<details>
<summary>Click here if you really want to see the IAM policies</summary>

**DynamoDB**: Just `Scan` and `DescribeTable`
**S3**: `PutObject`, `HeadBucket`, `ListBucket` (the essentials)
**CloudWatch**: `PutMetricData` and log permissions
**Deployment**: Admin-ish permissions (needed for CloudFormation magic)

</details>

## 📊 Monitoring

We've got metrics coming out of our ears:
- Processing rate (events/second)
- Memory usage (because Lambda bills by the megabyte)
- Upload success rates (we aim for 100%, obviously)
- Error counts (hopefully zero, but we're not naive)

Check CloudWatch for pretty graphs and the occasional alarm. 📈

## 🛠️ Local Development

```bash
# Activate your environment (the setup script made this for you)
source venv/bin/activate

# Run tests (because we're responsible developers)
pytest tests/ -v

# Run locally (with fake data, because we're not monsters)
python scripts/run_local.py

# Format your code (make it pretty)
./scripts/format_code.sh
```

## 🤔 Production Readiness (The Honest Truth)

### What We'd Add Before Going Full Production

**Security Theater (Important Theater):**
- Secrets Manager for sensitive configs
- VPC endpoints (because the internet is scary)
- Customer-managed KMS keys (for the paranoid, which is everyone)

**Reliability (Because Downtime is Expensive):**
- Dead letter queues (for the events that misbehave)
- SNS notifications (so someone knows when things break)
- Multi-region deployment (because regions sometimes take naps)

**Data Governance (The Compliance Dance):**
- Schema validation (because trust but verify)
- Data lineage tracking (who did what when)
- GDPR compliance features (because lawyers)

## 🏗️ Architecture Decisions (The Drama)

### Lambda vs AWS Glue: The Epic Showdown

**Winner**: Lambda (for now)

**Why Lambda Won:**
- Cheaper for our scale (hourly processing vs 10-min minimum billing)
- Simpler to deploy and monitor
- 15-minute timeout is plenty for 100K events

**Plot Twist**: When we hit 500K+ events/hour, **AWS Glue becomes the hero**. Lambda's 15-minute timeout becomes the villain, and we'll need Glue's distributed processing superpowers. It's like a superhero movie, but with more YAML files.

## 📈 Scaling (For When Things Get Serious)

**Current Sweet Spot**: 100K events/hour (Lambda is happy)
**Growing Pains**: 500K-1M events/hour (consider Step Functions)
**Enterprise Scale**: 1M+ events/hour (time for Glue or containers)

## 📁 Project Structure

```
processor/
├── src/                    # The good stuff
├── tests/                  # 70% coverage (I'm not perfect (lol))
├── scripts/                # Automation magic
├── cloudformation/         # Infrastructure as code
└── config/                 # Settings and knobs
```

## 🎯 Fun Facts

- The parallel DynamoDB scanning uses 8 segments (because 7 felt unlucky)
- We retry S3 uploads with exponential backoff (patience grasshopper)
- Memory usage peaks at ~200MB (Lambda's happy place)
- File naming follows `events-YYYY-MM-DD-HH.json` (ISO 8601 or bust!)
