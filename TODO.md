# TODO

## Engine/API
- [ ] Split Server and Worker into separate crates
- [ ] Enable Datafusion cluster for the worker with Ballista
- [ ] new virtual fields
- [ ] detect field datatype schema
- [ ] generalize datatype across multiple adapters
- [ ] make receipe receive arguments to filter eg dates, or other variables such tollerance amount 
## UI
- [ ] Source Data Sample Iteration scope with LLM
  - [ ] show preview of available list of fields
  - [ ] confirmation on field name (intellegent guess resolve underscore, dash etc)
  - [ ] confirmation on primary key
  - [ ] validation on recipe creation based on schema
 [ ] iteration preview from a source
- [ ] show live progress of a run

## Features
- [] result summary with stats

## Development Automation
- [ ] build sample data generation pipeline to verify