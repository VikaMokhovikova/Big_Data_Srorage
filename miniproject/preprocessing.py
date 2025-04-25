import pandas as pd

def clean_datasets(df, df_credits):
    all_ids_before = set(df['id'])
    df['homepage'] = df['homepage'].fillna('No homepage')
    df['tagline'] = df['tagline'].fillna('No tagline')

    # Drop rows where 'overview' or 'release_date' are missing
    df = df.dropna(subset=['overview', 'release_date'])

    # Drop rows where 'runtime' is missing
    df = df.dropna(subset=['runtime'])

    # Convert 'release_date' column to datetime format
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')

    # Ensure all values in 'keywords' column are strings
    df['keywords'] = df['keywords'].astype(str)
    df['homepage'] = df['homepage'].astype(str)
    df['original_language'] = df['original_language'].astype(str)
    df['original_title'] = df['original_title'].astype(str)
    df['overview'] = df['overview'].astype(str)
    df['status'] = df['status'].astype(str)
    df['tagline'] = df['tagline'].astype(str)
    df['title'] = df['title'].astype(str)
    all_ids_after = set(df['id'])
    del_ids = all_ids_before - all_ids_after
    df_credits = df_credits[~df_credits['movie_id'].isin(del_ids)]

    return df, df_credits
