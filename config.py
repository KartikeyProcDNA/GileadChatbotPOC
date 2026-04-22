from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
   
    azure_openai_api_key: str = ""
    azure_openai_endpoint: str = ""
    azure_openai_deployment: str = ""
    azure_openai_api_version: str = ""
    openai_max_tokens: int = 1500

 
    data_file_path: str = ""   # ← set your file path here

    sql_validation_enabled: bool = True
    query_row_limit: int = 500

    app_title: str = "Data Query API"
    app_version: str = "1.0.0"
    cors_origins: list[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
        "http://localhost:8501",
    ]
    fastapi_base_url: str = "http://localhost:8000"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
