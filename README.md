# parqeye

`parqeye` is a CLI Apache Parquet file viewer. It shows you the contents and metadata information that you can use to understand how the file is structured.

# Usage

Run `parqeye` by providing to the path to the `.parquet` file.

```
parqeye <path-to-parquet-file>
```

# Installation

## Direct Download

You can download the latest release from the [Releases](https://github.com/kaushik-srinivasan/parqeye/releases) page.

## Build from Source

You can build from source by downloading the repository and running the following command:

```
cargo build --release
```

# License

This package is released under the [MIT License](./LICENSE).

# Acknowledgements

- [csvlens](https://github.com/YS-L/csvlens) for the inspiration