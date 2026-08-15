# PersistentCollection Test Infrastructure

Testing is built on [Touchstone](https://www.nuget.org/packages/Touchstone), a runner-agnostic
framework in which test cases are defined once as descriptors and executed through any host.

## Projects

| Project          | Role                                                                                  |
|------------------|---------------------------------------------------------------------------------------|
| `Test.Shared`    | **Central source of truth.** Defines every test case as Touchstone suites (`PersistentCollectionSuites.All`) covering `PersistentQueue`, `PersistentStack`, `PersistentList`, and `PersistentDictionary` — positive and negative paths. |
| `Test.Automated` | Touchstone CLI runner (`Touchstone.Cli.ConsoleRunner`) that executes the shared suites and returns a non-zero exit code on failure. |
| `Test.Xunit`     | xUnit host (`Touchstone.XunitAdapter.TouchstoneFactBase`) running the same shared suites under `dotnet test`. |
| `Test.Nunit`     | NUnit host (`Touchstone.NunitAdapter.TouchstoneNunitBase`) running the same shared suites under `dotnet test`. |

The four `Test.<Queue|Stack|List|Dictionary>` projects are interactive console apps for manual
exploration and are independent of this automated infrastructure.

## Running

Console runner (colored, tabular output):

```
dotnet run --project src/Test.Automated/Test.Automated.csproj
dotnet run --project src/Test.Automated/Test.Automated.csproj -- --suite queue
dotnet run --project src/Test.Automated/Test.Automated.csproj -- --suite list --test remove_at
```

Suites: `queue`, `stack`, `list`, `dictionary`.

xUnit / NUnit via the standard test runner:

```
dotnet test src/Test.Xunit/Test.Xunit.csproj
dotnet test src/Test.Nunit/Test.Nunit.csproj
```

## Adding tests

Add a `TestCaseDescriptor` to the relevant suite in `Test.Shared`. All three runners pick it up
automatically — there is nothing to wire up in `Test.Automated`, `Test.Xunit`, or `Test.Nunit`.
