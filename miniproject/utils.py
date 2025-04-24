import ast
import json

import pandas as pd
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    insert,
)


def create_dimentions_movies(
    df: pd.DataFrame,
    column_name: str,
    dim_id_field: str = "id",
    db_path: str = "sqlite:///./tmdb_star.db",
) -> None:
    """
    Transforms a column with JSON lists of dictionaries into a dimension table and a linking table.

    Parameters:
        df: pd.DataFrame — initial DataFrame
        column_name: str — name of column to convert into dimension table (e.g. genres, keywords)
        dim_id_field: str — identifier key inside each JSON object (e.g. 'id')
        db_path: str — path to the SQLite database
    """
    df = df.copy()
    df[column_name] = df[column_name].apply(json.loads)

    all_items = [item for sublist in df[column_name] for item in sublist]
    df_dim = (
        pd.DataFrame(all_items)
        .drop_duplicates(subset=[dim_id_field])
        .reset_index(drop=True)
    )
    df_dim = df_dim[[dim_id_field, "name"]]
    df_dim.columns = ["id", "name"]

    id_dtype = (
        String if df_dim["id"].apply(lambda x: isinstance(x, str)).any() else Integer
    )

    link_data = []
    for _, row in df.iterrows():
        for item in row[column_name]:
            link_data.append({"fact_id": row["id"], "dim_id": item[dim_id_field]})
    df_link = pd.DataFrame(link_data)

    engine = create_engine(db_path)
    metadata = MetaData()
    metadata.reflect(bind=engine)

    dim_table = Table(
        f"dim_{column_name}",
        metadata,
        Column("id", id_dtype, primary_key=True),
        Column("name", String),
    )

    link_table = Table(
        f"link_{column_name}",
        metadata,
        Column(
            "fact_id", Integer, ForeignKey("fact_movies.movie_id"), primary_key=True
        ),
        Column(
            "dim_id", id_dtype, ForeignKey(f"dim_{column_name}.id"), primary_key=True
        ),
    )

    metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(dim_table.delete())
        conn.execute(link_table.delete())
        conn.execute(insert(dim_table), df_dim.to_dict(orient="records"))
        conn.execute(insert(link_table), df_link.to_dict(orient="records"))


def create_fact_movies_table(
    df: pd.DataFrame, db_path: str = "sqlite:///./tmdb_star.db"
) -> None:
    engine = create_engine(db_path)
    metadata = MetaData()

    Table(
        "fact_movies",
        metadata,
        Column("movie_id", Integer, primary_key=True),
        Column("title", String),
        Column("budget", BigInteger),
        Column("revenue", BigInteger),
        Column("popularity", Float),
        Column("release_date", Date),
        Column("runtime", Integer),
        Column("vote_average", Float),
        Column("vote_count", Integer),
        Column("homepage", String),
        Column("original_language", String),
        Column("original_title", String),
        Column("overview", String),
        Column("status", String),
        Column("tagline", String),
    )

    metadata.create_all(engine)

    df = df.rename(columns={"id": "movie_id"})[
        [
            "movie_id",
            "title",
            "budget",
            "revenue",
            "popularity",
            "release_date",
            "runtime",
            "vote_average",
            "vote_count",
            "homepage",
            "original_language",
            "original_title",
            "overview",
            "status",
            "tagline",
        ]
    ]

    df.to_sql("fact_movies", con=engine, if_exists="replace", index=False)


def parse_json_column(dataframe, column_name):
    try:
        return dataframe[column_name].apply(
            lambda x: json.loads(x) if pd.notna(x) else []
        )
    except Exception:
        return dataframe[column_name].apply(
            lambda x: ast.literal_eval(x) if pd.notna(x) else []
        )


def create_dimensions_credits(credits_df, db_path="sqlite:///tmdb_star.db"):
    engine = create_engine(db_path)
    metadata = MetaData()
    metadata.reflect(bind=engine)

    credits_df["cast_parsed"] = parse_json_column(credits_df, "cast")
    credits_df["crew_parsed"] = parse_json_column(credits_df, "crew")

    cast_table, crew_table = [], []

    for _, row in credits_df.iterrows():
        movie_id = row["movie_id"]
        for cast_member in row["cast_parsed"]:
            cast_table.append(
                {
                    "movie_id": movie_id,
                    "person_id": cast_member.get("id"),
                    "character": cast_member.get("character"),
                    "cast_id": cast_member.get("cast_id"),
                    "credit_id": cast_member.get("credit_id"),
                    "order": cast_member.get("order"),
                }
            )
        for crew_member in row["crew_parsed"]:
            crew_table.append(
                {
                    "movie_id": movie_id,
                    "person_id": crew_member.get("id"),
                    "department": crew_member.get("department"),
                    "job": crew_member.get("job"),
                    "credit_id": crew_member.get("credit_id"),
                }
            )

    cast_df = pd.DataFrame(cast_table)
    crew_df = pd.DataFrame(crew_table)

    # Собираем таблицу персон
    cast_persons = pd.DataFrame(
        row for row in cast_table if row["person_id"] is not None
    )[["person_id"]].drop_duplicates()
    crew_persons = pd.DataFrame(
        row for row in crew_table if row["person_id"] is not None
    )[["person_id"]].drop_duplicates()
    all_ids = pd.concat([cast_persons, crew_persons]).drop_duplicates()

    # Добавляем имена и гендер (при наличии)
    persons_data = {}
    for row in credits_df.itertuples():
        for entry in row.cast_parsed + row.crew_parsed:
            pid = entry.get("id")
            if pid is not None:
                persons_data[pid] = {
                    "person_id": pid,
                    "name": entry.get("name"),
                    "gender": entry.get("gender"),
                }

    persons_df = pd.DataFrame.from_dict(persons_data, orient="index").drop_duplicates(
        subset=["person_id"]
    )

    # Определим таблицы
    dim_persons = Table(
        "dim_persons",
        metadata,
        Column("person_id", Integer, primary_key=True),
        Column("name", String),
        Column("gender", Integer),
    )

    link_cast = Table(
        "link_cast",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("movie_id", Integer, ForeignKey("fact_movies.movie_id")),
        Column("person_id", Integer, ForeignKey("dim_persons.person_id")),
        Column("cast_id", Integer),
        Column("character", String),
        Column("credit_id", String),
        Column("order", Integer),
    )

    link_crew = Table(
        "link_crew",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("movie_id", Integer, ForeignKey("fact_movies.movie_id")),
        Column("person_id", Integer, ForeignKey("dim_persons.person_id")),
        Column("department", String),
        Column("job", String),
        Column("credit_id", String),
    )

    metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(dim_persons.delete())
        conn.execute(link_cast.delete())
        conn.execute(link_crew.delete())
        conn.execute(insert(dim_persons), persons_df.to_dict(orient="records"))
        conn.execute(insert(link_cast), cast_df.to_dict(orient="records"))
        conn.execute(insert(link_crew), crew_df.to_dict(orient="records"))
