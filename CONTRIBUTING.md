# Contributing Guidelines

This project would not be here without its community, so contributions have and will always be welcome!

## Issues

When reporting a bug, please create an [issue](https://github.com/gouline/issues) and provide as much content as humanly possible:

* Package version (`dbt-metabase --version` or `pip show dbt-metabase`)
* Environment details, e.g. operating system, cloud provider, database
* Steps to reproduce
* Expected vs actual results
* Sample code, logs, screenshots

## Pull Requests

Code contributions must be reviewed by the maintainer in a [pull request](https://github.com/gouline/pulls). This project only has one maintainer, so please be patient if the review process takes days or weeks.

Unless your change is a bug fix or an incremental addition, consider proposing your approach in an issue first. While your contributions are appreciated, not everything is suitable for this project, and seeking feedback in advance avoids wasting your time implementing something that gets rejected.

### Validation

While checks and tests are run automatically on pull requests, GitHub Actions requires maintainers to manually approve new contributors for security reasons. This means you could spend days waiting for a code review only to be immediately rejected because validation failed. To avoid this, please **run checks locally before you commit**!

To execute only checks (you can also run each check separately, e.g. `check-fmt`):

```
make check
```

To execute only tests:

```
make test
```

The most convenient way to fix formatting and imports, and run checks and tests, with one command:

```
make pre
```

### Tests

Any code you contribute **must have unit tests**. Bug fixes in particular require at least one test case that fails before your fix and succeeds afterwards. This helps communicate how your contribution works and ensures no future changes inadvertently break it.

### Sandbox

While developing, it can be useful to have a sandbox with Metabase, dbt and PostgreSQL running locally to test your changes. To start it in [Docker Compose](https://docs.docker.com/compose/), execute the following (see [.env](./sandbox/.env) for ports and credentials):

```
make sandbox-up
```

To execute dbt-metabase commands against it:

```
make sandbox-models
make sandbox-exposures
```

Once you are finished, stop the sandbox:

```
make sandbox-down
```

## Code of Conduct

All contributors are expected to follow the [PSF Code of Conduct](https://www.python.org/psf/conduct/).

