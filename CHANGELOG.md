# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0]

### Added

- Support for large Parquet files with large row counts in the data preview ([#43](https://github.com/kaushiksrini/parqeye/pull/43))
- `g`/`G` shortcuts to jump to the top/bottom of the data preview ([#43](https://github.com/kaushiksrini/parqeye/pull/43))
- PyPI package and build workflow, so you can invoke `uvx parqeye` ([#40](https://github.com/kaushiksrini/parqeye/pull/40))

### Fixed

- Incorrect Min/Max aggregation in the Schema tab ([#44](https://github.com/kaushiksrini/parqeye/pull/44))
- Column file offset and distinct count now show `N/A` when not set in the file metadata ([#46](https://github.com/kaushiksrini/parqeye/pull/46))
- PyPI workflow builds for Linux `manylinux_2_28` ([#41](https://github.com/kaushiksrini/parqeye/pull/41))

### Changed

- Lazily load page info to speed up startup ([#32](https://github.com/kaushiksrini/parqeye/pull/32))

## [0.1.0]

### Added

- Properties tab in the metadata section ([#28](https://github.com/kaushiksrini/parqeye/pull/28))
- File handling errors for invalid/erroneous files ([#31](https://github.com/kaushiksrini/parqeye/pull/31))

### Fixed

- Horizontal column scrolling in the data preview ([#30](https://github.com/kaushiksrini/parqeye/pull/30))
- Clippy errors ([#31](https://github.com/kaushiksrini/parqeye/pull/31))
- Metadata paragraph line splitting to support the scroll feature instead of wrapping ([#28](https://github.com/kaushiksrini/parqeye/pull/28))

### Changed

- Updated `parquet`/`arrow-rs` dependencies ([#33](https://github.com/kaushiksrini/parqeye/pull/33))
- Updated `parquet-testing` submodule to latest ([#27](https://github.com/kaushiksrini/parqeye/pull/27))

## [0.0.2]

- Initial tagged release.

[0.2.0]: https://github.com/kaushiksrini/parqeye/releases/tag/v0.2.0
[0.1.0]: https://github.com/kaushiksrini/parqeye/releases/tag/v0.1.0
[0.0.2]: https://github.com/kaushiksrini/parqeye/releases/tag/v0.0.2
