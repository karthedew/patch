from dataclasses import dataclass


@dataclass(frozen=True)
class RepoConfig:
    owner: str
    repo: str
    language: str
    domain: str

    @property
    def full_name(self) -> str:
        return f"{self.owner}/{self.repo}"


REPOS = [
    # Python - Web
    RepoConfig("fastapi", "fastapi", "python", "web"),
    RepoConfig("django", "django", "python", "web"),
    RepoConfig("pallets", "flask", "python", "web"),
    # Python - Data Engineering
    RepoConfig("apache", "airflow", "python", "data-engineering"),
    RepoConfig("apache", "arrow", "python", "data-engineering"),
    RepoConfig("dask", "dask", "python", "data-engineering"),
    RepoConfig("prefecthq", "prefect", "python", "data-engineering"),
    RepoConfig(
        "great-expectations",
        "great_expectations",
        "python",
        "data-engineering",
    ),
    RepoConfig("sqlalchemy", "sqlalchemy", "python", "data-engineering"),
    # Python - Dataframe
    RepoConfig("pola-rs", "polars", "python", "dataframe"),
    RepoConfig("pandas-dev", "pandas", "python", "dataframe"),
    # Python - Scientific
    RepoConfig("numpy", "numpy", "python", "scientific"),
    RepoConfig("scipy", "scipy", "python", "scientific"),
    RepoConfig("scikit-learn", "scikit-learn", "python", "scientific"),
    RepoConfig("matplotlib", "matplotlib", "python", "scientific"),
    # Go - Web
    RepoConfig("gin-gonic", "gin", "go", "web"),
    RepoConfig("go-chi", "chi", "go", "web"),
    RepoConfig("labstack", "echo", "go", "web"),
    # Go - Systems / Data
    RepoConfig("golang", "go", "go", "systems"),
    RepoConfig("kubernetes", "kubernetes", "go", "systems"),
    RepoConfig("prometheus", "prometheus", "go", "data-engineering"),
    RepoConfig("apache", "arrow-go", "go", "data-engineering"),
    # Rust - Web
    RepoConfig("actix", "actix-web", "rust", "web"),
    RepoConfig("tokio-rs", "axum", "rust", "web"),
    # Rust - Data Engineering
    RepoConfig("apache", "arrow-rs", "rust", "data-engineering"),
    RepoConfig("apache", "datafusion", "rust", "data-engineering"),
    RepoConfig("delta-io", "delta-rs", "rust", "data-engineering"),
    # Rust - Systems
    RepoConfig("tokio-rs", "tokio", "rust", "systems"),
    RepoConfig("serde-rs", "serde", "rust", "systems"),
    RepoConfig("pola-rs", "polars", "rust", "systems"),
    # JavaScript / TypeScript - Web
    RepoConfig("expressjs", "express", "javascript", "web"),
    RepoConfig("fastify", "fastify", "javascript", "web"),
    RepoConfig("vercel", "next.js", "typescript", "web"),
    RepoConfig("nestjs", "nest", "typescript", "web"),
    # JavaScript / TypeScript - Systems
    RepoConfig("nodejs", "node", "javascript", "systems"),
    RepoConfig("microsoft", "TypeScript", "typescript", "systems"),
    # C/C++ - Data Engineering
    RepoConfig("apache", "arrow", "c++", "data-engineering"),
    RepoConfig("duckdb", "duckdb", "c++", "data-engineering"),
    RepoConfig("postgres", "postgres", "c", "data-engineering"),
    # C/C++ - Systems / Scientific
    RepoConfig("redis", "redis", "c", "systems"),
    RepoConfig("opencv", "opencv", "c++", "scientific"),
    RepoConfig("llvm", "llvm-project", "c++", "systems"),
    RepoConfig("google", "leveldb", "c++", "systems"),
]


REPO_INDEX = {cfg.full_name: cfg for cfg in REPOS}
