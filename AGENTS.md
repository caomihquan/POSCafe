# Repository Guidelines

## Project Structure & Module Organization
This repository is a .NET 10 solution organized around service boundaries. Core code lives under `src/`:

- `src/PosCafe.AppHost/` for Aspire orchestration and local app hosting
- `src/PosCafe.ServiceDefaults/` for shared hosting, telemetry, and resilience defaults
- `src/Gateway/PosCafe.ApiGateway/` for the API gateway
- `src/Services/<ServiceName>/` for service-specific `Api`, `Application`, `Domain`, and `Infrastructure` projects
- `BuildingBlocks/BuildingBlocks/` for shared building blocks
- `tests/` for automated tests

The solution file is `PosCafe.slnx`.

## Build, Test, and Development Commands
Use `dotnet` from the repository root:

- `dotnet restore` - restore all NuGet dependencies
- `dotnet build PosCafe.slnx` - compile the full solution
- `dotnet test PosCafe.slnx` - run all tests in the solution
- `dotnet run --project src/PosCafe.AppHost/PosCafe.AppHost.csproj` - start the local Aspire app host
- `dotnet run --project src/Gateway/PosCafe.ApiGateway/PosCafe.ApiGateway.csproj` - run the gateway directly if needed

## Coding Style & Naming Conventions
The codebase uses SDK-style C# projects with `Nullable` enabled and `ImplicitUsings` enabled. Follow the existing C# conventions:

- Use 4-space indentation
- Prefer PascalCase for types, methods, and public members
- Use camelCase for local variables and parameters
- Name projects and namespaces with the `PosCafe.<Area>.<Layer>` pattern, such as `PosCafe.Catalog.Application`

Keep files small and aligned to the domain layer they belong to. Place shared logic in `BuildingBlocks/` only when it is truly cross-cutting.

## Testing Guidelines
The repository includes a `tests/` folder, but no test framework is currently visible in the tracked projects. When adding tests, keep them close to the feature area and name them clearly, for example:

- `OrderServiceTests.cs`
- `CreatesOrder_WhenInventoryIsAvailable`

Run the full suite with `dotnet test PosCafe.slnx` before opening a pull request.

## Commit & Pull Request Guidelines
No commit-message convention is enforced in the current repository layout, so keep commits short, imperative, and scoped to one change, such as `Add catalog service validation`.

Pull requests should include:

- A brief summary of the change
- Any relevant test results
- Screenshots or sample requests only when API behavior or contracts change
- Notes about configuration, migrations, or new service wiring

## Configuration Tips
Service projects include `appsettings.json` and `appsettings.Development.json`. Avoid committing secrets; use local development settings or environment variables for machine-specific values.
