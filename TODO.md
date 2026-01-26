# TODO

## Priority 1: Must-Have for Public Release

### Results & Feedback
- [ ] **Result summary with stats** - Display match rate, unmatched counts, and potential issues after each run
- [ ] **Live progress indicator** - Show real-time progress during reconciliation runs

### Source Setup Experience
- [ ] **Field preview** - Display available columns when configuring a data source

## Priority 2: Improves Adoption

### Guided Setup
- [x] **Primary key confirmation** - Prompt user to confirm detected primary key before matching
- [x] **Smart field name resolution** - Auto-resolve common variations (underscores, dashes, casing)
- [x] **Recipe schema validation** - Validate match rules against source schema before running

### Source Preview
- [x] **Row preview from source** - Show sample rows when exploring a data source

## Priority 3: Future Enhancements

### Engine/API
- [ ] **Split Server and Worker** - Separate API server from reconciliation worker for independent scaling
- [ ] **Ballista cluster support** - Enable DataFusion distributed execution for large datasets
- [ ] **Virtual fields** - Define computed columns in recipes
- [ ] **Datatype detection** - Infer field types from source data
- [ ] **Unified type system** - Normalize datatypes across different adapters
- [ ] **Adapter abstraction** - Ensure clean interface for adding new connectors (S3, GCS, MySQL, BigQuery, Snowflake)
- [ ] **Recipe parameters** - Accept runtime arguments for dates, tolerance amounts, filters

## Development Automation
- [ ] **Sample data generator** - Build pipeline to generate test datasets for verification
