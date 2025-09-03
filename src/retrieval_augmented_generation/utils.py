import requests
from tqdm import tqdm
from pathlib import Path
from .types import PathLike
import typing


def download(
    url: str, *, output: PathLike, verbose: bool = False, chunk_size: int = 8192
):
    output = Path(output)

    with requests.get(url, stream=True) as response:
        response.raise_for_status()  # ensure request was successful
        total_size: int = int(response.headers.get("content-length", 0))

        with output.open("wb") as file:
            iterator: typing.Iterator[bytes] = response.iter_content(
                chunk_size=chunk_size
            )

            for chunk in tqdm(
                iterator,
                total=total_size // 8192 + 1 if total_size else None,
                unit="chunk",
                desc=f"Downloading {output.name}",
                disable=not verbose,
            ):
                if chunk:  # skip keep-alive chunks
                    file.write(chunk)
