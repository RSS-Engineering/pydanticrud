
# Change Log
All notable changes to this project will be documented in this file.
 
The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).
 
## [1.0.0] - 2024-01-15
 
Added support for Pydantic V2 (version - ^2.5).
 
### Added

- Test coverage for Python versions `3.11` and `3.12`

### Changed
- Python base version from `3.7` to `3.8`.
- Changed Pydantic version from `^1.8.2` to `^2.5`.
- Updated Model validation function from `parse_obj` to `model_validate`.
- Renamed backend initialization class from `Config` to `db_config` to follow pydantic's naming convention.
- Updated method for generation of dictionary from `dict` to `model_dump`.

### Fixed


## [0.4.2] - 2023-11-16
 
Added count() for dynamo backend that returns integer count as total.
