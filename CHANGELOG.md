# Changelog

Versions follow [CalVer](https://calver.org). This changelog follows the style guides described on
[https://keepachangelog.com/en/1.0.0/](https://keepachangelog.com/en/1.0.0/)

## 23.3.1 (Not yet released)

TODO.

______________________________________________________________________

## 23.3.0 (2023-03-10)

### Added

- Use two-digits to indicate the year in the project version number.
- Fix the exit codes returned by the CLI when invoking the development tasks.
- Update the GitHub action that executes the package tests.
- Add task `format-docs` to format documentation source files in Markdown format.
- Add support to use the Python Language Server to enhance the development experience with Jupyter
  notebooks.
- Create a new task to launch a Jupyter lab server with some additional directories as part of the
  system environment variables.
- Update the target Python versions when applying `black`.
- Add a pre-commit hook and config file to use the `mdformat` library.
- Add a new extra collection of the package: `jupyter-enhancements`.
- Upgrade dependencies, i.e., `poetry.lock` file.

### Changed

- Update the minimum Python version to `3.8` and the maximum to `3.10` (inclusive).

______________________________________________________________________

## 2021.2.0 (2021-08-19)

### Added

- Add `rope` library for development purposes.
- Add documentation source code using `sphinx` and `sphinx_rtd_theme`.
- Add a GitHub action to run the tests on every push to the main branch.
- Enable support to run code quality checks using the `pre-commit` library.
- Add development tasks to install, uninstall, and upgrade the project package.

### Changed

- Do not force the project package name to be the same as the project name in the
  `src/pyproject/__init__.py`. We must recover the package metadata stating the project name in the
  code.
- Make the package metadata a public name of the project package.
- Use `click` to create the command line interfaces for development tasks.

### Removed

- Remove redundant development tasks.

### Fixed

- Fix tasks to install, uninstall, and upgrade the project package.

______________________________________________________________________

## 21.1.0 (2021-07-24)

### Added

- Define the project structure and fundamental elements.
