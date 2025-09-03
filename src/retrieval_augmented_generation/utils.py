import requests
from tqdm.auto import tqdm
from pathlib import Path
from .types import PathLike
import typing
import gzip
import tarfile
from urllib.parse import urlparse


def download(
    url: str,
    *,
    output_dir: PathLike,
    verbose: bool = False,
    chunk_size: int = 8192,
    override: bool = False,
) -> Path:
    output_dir = Path(output_dir)
    if output_dir.exists() and output_dir.is_file():
        raise ValueError(
            f"Output path '{output_dir}' exists and is a file, not a directory"
        )
    output_dir.mkdir(parents=True, exist_ok=True)

    file_name: typing.Optional[str] = urlparse(url).path.split("/")[-1]

    if not file_name or file_name == "/":
        file_name = "downloaded_file"

    output: Path = output_dir.joinpath(file_name)

    if not override and output.exists():
        return output

    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()  # ensure request was successful
            total_size: int = int(response.headers.get("content-length", 0))

            with open(output, "wb") as f:
                iterator: typing.Iterator[bytes] = response.iter_content(
                    chunk_size=chunk_size
                )

                for chunk in tqdm(
                    iterator,
                    total=total_size // chunk_size + 1 if total_size else None,
                    unit="chunk",
                    desc=f"Downloading {output.name}",
                    disable=not verbose,
                ):
                    if chunk:  # skip keep-alive chunks
                        f.write(chunk)

        return output

    except BaseException:
        if output.exists():
            output.unlink()
        raise


def unpack(
    file: PathLike,
    *,
    output_dir: typing.Optional[PathLike] = None,
    verbose: bool = False,
    remove_after: bool = False,
    override: bool = False,
) -> Path:
    file = Path(file)
    if not file.exists():
        raise ValueError(f"Input file '{file}' does not exist")

    if output_dir is None:
        output_dir = file.parent
    else:
        output_dir = Path(output_dir)
    if output_dir.exists() and output_dir.is_file():
        raise ValueError(
            f"Output path '{output_dir}' exists and is a file, not a directory"
        )
    output_dir.mkdir(parents=True, exist_ok=True)

    if file.name.endswith(".tar.gz"):
        output_path = output_dir / file.name[:-7]  # Remove .tar.gz
    elif file.name.endswith(".gz"):
        output_path = output_dir.joinpath(file.stem)
    else:
        raise ValueError("File must be .gz or .tar.gz")

    if not override and output_path.exists():
        return output_path

    try:
        if file.name.endswith(".tar.gz"):
            if verbose:
                print(f"Extracting {file.name} to {output_dir}")

            with tarfile.open(file, "r:gz") as tar:
                tar.extractall(path=output_dir)

        elif file.name.endswith(".gz"):
            if verbose:
                print(f"Extracting {file.name} to {output_path}")

            with gzip.open(file, "rb") as f_in:
                output_path.write_bytes(f_in.read())

        if verbose:
            print(f"Successfully extracted to {output_path}")

        if remove_after:
            if verbose:
                print(f"Removing original file {file}")
            file.unlink()

        return output_path

    except BaseException:
        if output_path.exists():
            output_path.unlink()
