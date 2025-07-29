def transform(df):
    df_transformed = df[['name.first', 'name.last', 'email', 'location.city', 'location.country']]
    df_transformed.columns = ['first_name', 'last_name', 'email', 'city', 'country']
    return df_transformed

"""def transform(df):
    df = df.dropna()
    df['Total'] = df['Quantity'] * df['Price']
    return df"""