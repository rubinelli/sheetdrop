import sqlalchemy

def create_engine(url: str) -> sqlalchemy.engine.Engine:
    """Create a database engine for use within FastApi
    Parameters:
        url: str
            The URL of the database
    Returns:
        sqlalchemy.engine.Engine
            The engine for the database
    """
    return sqlalchemy.create_engine(url)

def create_tables(engine: sqlalchemy.engine.Engine, schema: str, table_prefix: str) -> None:
    """Create the tables in the database
    Parameters:
        engine: sqlalchemy.engine.Engine
            The engine for the database
        schema: str
            The schema in which to create the tables
        table_prefix: str
            The prefix of the table names
    """
    metadata = sqlalchemy.MetaData(schema=schema)
    prefix = table_prefix or ""
    file_status = sqlalchemy.Table(
        f"{prefix}file_status",
        metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, autoincrement=True, primary_key=True),
        sqlalchemy.Column("file_id", sqlalchemy.String, index=True, nullable=False),
        sqlalchemy.Column("status", sqlalchemy.String, nullable=False),
        sqlalchemy.Column("status_detail", sqlalchemy.String),
    )

    metadata.create_all(engine)