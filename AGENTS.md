# Repository Guidelines

## Project Structure & Module Organization

This is a Java 21, Maven multi-module Spring Boot service. The root `pom.xml` is the aggregator and dependency-management entry point. Modules are split by responsibility:

- `cardshop-trade-engine-domain`: domain model and rules; keep it free of Spring, MyBatis, Redis, and RabbitMQ dependencies.
- `cardshop-trade-engine-application`: application services and ports.
- `cardshop-trade-engine-api`: WebFlux controllers and request/response DTOs.
- `cardshop-trade-engine-infrastructure`: Redis, RabbitMQ, MyBatis, Nacos-bound configuration, and adapters.
- `cardshop-trade-engine-job`: XXL-Job handlers and background entry points.
- `cardshop-trade-engine-migration`: Liquibase changelogs, currently under `src/main/resources/db/changelog/`.
- `cardshop-trade-engine-test-support`: shared integration-test fixtures.
- `cardshop-trade-engine-bootstrap`: application startup, profiles, Actuator, and runtime configuration.

Put production code in each module's `src/main/java`, tests in `src/test/java`, and resources in `src/main/resources`. Do not commit `target/`, logs, IDE files, or compiled classes.

## Build, Test, and Development Commands

- `mvn clean package`: build all modules and run tests.
- `mvn test`: run the full test suite.
- `mvn -pl cardshop-trade-engine-bootstrap -am test`: test the bootstrap module plus required dependencies.
- `./scripts/dev-local.sh`: run the service locally with `local` profile and external middleware auto-configuration disabled.
- `APP_PROFILE=dev NACOS_SERVER_ADDR=127.0.0.1:8848 NACOS_NAMESPACE=cardshop-trade-engine-dev NACOS_GROUP=TRADE_ENGINE_DEV mvn -pl cardshop-trade-engine-bootstrap -am spring-boot:run`: run locally with the dev profile.

Use JDK 21 and Maven 3.9 or newer.

## Coding Style & Naming Conventions

Follow existing Spring Java style: four-space indentation, package names under `com.cardshop.trade.engine`, and descriptive class names such as `TradeEngineProperties`. Keep configuration properties kebab-cased under `trade.engine.*`. Prefer constructor injection for Spring components and keep cross-module dependencies aligned with the module responsibilities above.

## Development Rules

When implementation details require changing the agreed approach, update both design documents in `docs/` before changing code: `docs/交行直充业务系统高可用方案设计.md` and `docs/交行直充业务系统详细设计.md`.

Before implementing interfaces covered by `docs/交行直充接口技术文档-V1.1.docx`, inspect the legacy implementation under `/Users/mac/Documents/codeSpace/cardshop` and keep the new service's business behavior basically consistent with the existing system unless the design documents or the user explicitly require a change.

If requirements, interface semantics, legacy behavior, or design details are ambiguous or unclear, stop and confirm with the user before implementing that part.

Code must include detailed Chinese comments for important classes, methods, flows, and non-obvious decisions. Comments should link to the relevant section in `docs/交行直充业务系统详细设计.md`. DTO class comments and field comments must stay consistent with the corresponding DDL `COMMENT` text.

Prefer Java 21 language and library features where they make the code clearer, safer, or simpler. Key business, routing, Redis, MQ, MySQL, and compensation paths must log enough context to support production troubleshooting, without writing secrets or sensitive payloads.

## Testing Guidelines

Tests use JUnit 5, Spring Boot Test, AssertJ, and Maven Surefire. Name unit and slice tests `*Test` and mirror the production package path. Add focused tests near the changed module; use `cardshop-trade-engine-test-support` only for reusable fixtures.

## Commit & Pull Request Guidelines

Git history currently contains only `Initial commit`, so no strict convention is established. Use concise imperative subjects, optionally scoped by module, for example `bootstrap: add readiness probe test`. Pull requests should include a short summary, linked issue or task, test evidence, and notes for profile, Nacos, Liquibase, Redis, MQ, or MySQL changes.

## Security & Configuration Tips

Do not commit credentials or environment-specific secrets. Nacos, Redis, RabbitMQ, and MySQL connection details must come from environment variables, Nacos, or deployment secrets. Treat production Redis writes carefully; verify migration, MQ, MySQL, and gray-routing readiness before enabling write behavior.
