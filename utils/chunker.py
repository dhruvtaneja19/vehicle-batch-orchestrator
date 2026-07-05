def split_into_chunks(df, chunk_size):

    chunks = []

    for i in range(0, len(df), chunk_size):
        chunks.append(df.iloc[i:i + chunk_size])

    return chunks
