from pathlib import Path

def save_chunks(chunks, output_folder):

    output_folder.mkdir(
        parents=True,
        exist_ok=True
    )

    saved_files = []
    for index, chunk in enumerate(chunks):

        filename = output_folder / f"batch_{index+1}.csv"

        chunk.to_csv(filename, index=False)
        saved_files.append(str(filename))

    return saved_files
