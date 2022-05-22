# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2022-05-21
### Added

### Changed
- Workaround SEGFAULT in Chromium by restarting playwright browser after
  every 5 pages downloaded
- Use Xvfb in docker container to run full Chromium (GUI and everything)
- Change search paths for configuration file to $HOME/.config and /etc

### Removed

## [0.1.0] - 2022-05-20
### Added
- Interfaces to collect quant ratings via playwright
- Export to parquet
- Export to database

### Changed

### Removed

[Unreleased]: https://github.com/penny-vault/import-sa-quant-rank/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/penny-vault/import-sa-quant-rank/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/penny-vault/import-sa-quant-rank/releases/tag/v0.1.0
