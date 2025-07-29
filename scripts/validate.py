def validate(df):
    if df.empty:
        raise ValueError("DataFrame is empty!")
    required_cols = ['name.first', 'email']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")