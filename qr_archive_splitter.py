#!/usr/bin/env python3
"""
Split any file into QR-code PNGs and reconstruct it later.

Examples:
  python qr_archive_splitter.py encode backup.zip
  python qr_archive_splitter.py decode backup_img

Dependencies:
  pip install qrcode[pil] opencv-python pillow
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
import re
import sys
from pathlib import Path
from typing import Any


FORMAT_MAGIC = "F2QR1"
MANIFEST_MAGIC = "F2QRM1"
DEFAULT_CHUNK_BYTES = 600
DEFAULT_BOX_SIZE = 12
DEFAULT_BORDER = 8
DEFAULT_MASK_PATTERN = 0
MASK_PATTERNS = tuple(range(8))
ECC_LEVELS = ("L", "M", "Q", "H")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def safe_stem(path: Path) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", path.stem).strip("._") or "file"


def qr_ecc_constant(level: str) -> int:
    try:
        import qrcode
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency: qrcode. Install with: pip install qrcode[pil]"
        ) from exc

    constants = qrcode.constants
    return {
        "L": constants.ERROR_CORRECT_L,
        "M": constants.ERROR_CORRECT_M,
        "Q": constants.ERROR_CORRECT_Q,
        "H": constants.ERROR_CORRECT_H,
    }[level]


def make_qr_image(qr_text: str, ecc: int, box_size: int, border: int, mask_pattern: int):
    import qrcode

    qr = qrcode.QRCode(
        version=None,
        error_correction=ecc,
        box_size=box_size,
        border=border,
        mask_pattern=mask_pattern,
    )
    qr.add_data(qr_text)
    qr.make(fit=True)
    return qr.make_image(fill_color="black", back_color="white").convert("RGB")


def choose_decoder_friendly_qr(
    qr_text: str,
    ecc: int,
    box_size: int,
    border: int,
    preferred_mask: int,
    self_check: bool,
):
    mask_order = (preferred_mask,) + tuple(mask for mask in MASK_PATTERNS if mask != preferred_mask)

    if not self_check:
        return make_qr_image(qr_text, ecc, box_size, border, preferred_mask), preferred_mask, False

    try:
        import cv2
        import numpy as np
    except ImportError as exc:
        raise SystemExit(
            "QR self-check needs opencv-python and numpy. Install with: pip install opencv-python"
        ) from exc

    detector = cv2.QRCodeDetector()
    last_image = None
    last_mask = preferred_mask

    for mask_pattern in mask_order:
        image = make_qr_image(qr_text, ecc, box_size, border, mask_pattern)
        last_image = image
        last_mask = mask_pattern
        decoded_text, _points, _straight = detector.detectAndDecode(np.array(image))
        if decoded_text == qr_text:
            return image, mask_pattern, True

    return last_image, last_mask, False


def build_qr_text(metadata: dict[str, Any], payload: bytes) -> str:
    encoded_payload = base64.b85encode(payload).decode("ascii")
    encoded_metadata = json.dumps(metadata, sort_keys=True, separators=(",", ":"))
    return f"{FORMAT_MAGIC}\n{encoded_metadata}\n{encoded_payload}"


def build_manifest_qr_text(manifest: dict[str, Any]) -> str:
    encoded_manifest = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
    return f"{MANIFEST_MAGIC}\n{encoded_manifest}"


def parse_manifest_qr_text(text: str) -> dict[str, Any]:
    try:
        magic, manifest_text = text.split("\n", 1)
    except ValueError as exc:
        raise ValueError("Manifest QR does not contain the expected two lines") from exc

    if magic != MANIFEST_MAGIC:
        raise ValueError(f"Unsupported manifest QR magic: {magic!r}")

    return json.loads(manifest_text)


def parse_qr_text(text: str) -> tuple[dict[str, Any], bytes]:
    try:
        magic, metadata_text, encoded_payload = text.split("\n", 2)
    except ValueError as exc:
        raise ValueError("QR payload does not contain the expected three lines") from exc

    if magic != FORMAT_MAGIC:
        raise ValueError(f"Unsupported QR format magic: {magic!r}")

    metadata = json.loads(metadata_text)
    payload = base64.b85decode(encoded_payload.encode("ascii"))
    return metadata, payload


def iter_file_chunks(path: Path, chunk_bytes: int):
    with path.open("rb") as file:
        while True:
            chunk = file.read(chunk_bytes)
            if not chunk:
                break
            yield chunk


def write_manifest_qr(
    manifest: dict[str, Any],
    output_dir: Path,
    ecc: int,
    box_size: int,
    border: int,
    preferred_mask: int,
    self_check: bool,
) -> int:
    manifest_text = build_manifest_qr_text(manifest)
    image, actual_mask, verified = choose_decoder_friendly_qr(
        manifest_text,
        ecc,
        box_size,
        border,
        preferred_mask,
        self_check,
    )
    if self_check and not verified:
        raise SystemExit("Could not create a decoder-friendly manifest QR.")

    image.save(output_dir / "manifest.png")
    return actual_mask


def encode_file(args: argparse.Namespace) -> None:
    try:
        import qrcode
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency: qrcode. Install with: pip install qrcode[pil]"
        ) from exc

    input_path = Path(args.input).expanduser().resolve()
    if not input_path.is_file():
        raise SystemExit(f"Input file not found: {input_path}")

    if args.chunk_bytes < 128:
        raise SystemExit("--chunk-bytes must be at least 128")

    file_size = input_path.stat().st_size
    if file_size == 0:
        raise SystemExit("Empty files are not supported.")

    output_dir = (
        Path(args.output).expanduser().resolve()
        if args.output
        else input_path.with_name(f"{safe_stem(input_path)}_img")
    )
    if output_dir.exists() and any(output_dir.iterdir()) and not args.force:
        raise SystemExit(f"Output folder is not empty, use --force to replace generated files: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    if args.force:
        for old_path in output_dir.glob("part_*_of_*.png"):
            old_path.unlink()
        old_manifest = output_dir / "manifest.json"
        if old_manifest.exists():
            old_manifest.unlink()
        old_manifest_qr = output_dir / "manifest.png"
        if old_manifest_qr.exists():
            old_manifest_qr.unlink()

    file_hash = sha256_file(input_path)
    part_count = math.ceil(file_size / args.chunk_bytes)
    width = len(str(part_count))
    ecc = qr_ecc_constant(args.ecc)

    manifest = {
        "format": FORMAT_MAGIC,
        "file_name": input_path.name,
        "file_size": file_size,
        "file_sha256": file_hash,
        "part_count": part_count,
        "chunk_bytes": args.chunk_bytes,
        "payload_encoding": "base85",
        "ecc": args.ecc,
        "preferred_mask_pattern": args.mask_pattern,
        "self_check": not args.no_self_check,
    }

    for part_index, chunk in enumerate(iter_file_chunks(input_path, args.chunk_bytes), 1):
        metadata = {
            "part_index": part_index,
            "part_count": part_count,
            "payload_encoding": "base85",
        }
        qr_text = build_qr_text(metadata, chunk)
        image, actual_mask, verified = choose_decoder_friendly_qr(
            qr_text,
            ecc,
            args.box_size,
            args.border,
            args.mask_pattern,
            not args.no_self_check,
        )
        if not args.no_self_check and not verified:
            raise SystemExit(
                f"Could not create a decoder-friendly QR for part {part_index}. "
                "Try lowering --chunk-bytes or using --ecc M."
            )
        out_path = output_dir / f"part_{part_index:0{width}d}_of_{part_count:0{width}d}.png"
        image.save(out_path)

        if args.verbose:
            print(f"Wrote {out_path.name} ({part_index}/{part_count}, mask {actual_mask})")

    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    manifest_mask = write_manifest_qr(
        manifest,
        output_dir,
        ecc,
        args.box_size,
        args.border,
        args.mask_pattern,
        not args.no_self_check,
    )

    print(f"Created {part_count} part QR image(s) and manifest.png in: {output_dir}")
    print(f"Original SHA-256: {file_hash}")
    if args.verbose:
        print(f"Wrote manifest.png (mask {manifest_mask})")


def decode_image(path: Path) -> str | None:
    try:
        import cv2
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency: opencv-python. Install with: pip install opencv-python"
        ) from exc

    image = cv2.imread(str(path))
    if image is None:
        return None

    detector = cv2.QRCodeDetector()
    text, _points, _straight = detector.detectAndDecode(image)
    return text or None


def load_manifest_json(input_dir: Path) -> dict[str, Any] | None:
    manifest_path = input_dir / "manifest.json"
    if not manifest_path.is_file():
        return None

    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(f"Could not read manifest.json: {exc}") from exc


def decode_folder(args: argparse.Namespace) -> None:
    input_dir = Path(args.input).expanduser().resolve()
    if not input_dir.is_dir():
        raise SystemExit(f"Input folder not found: {input_dir}")

    image_paths = sorted(
        path
        for path in input_dir.iterdir()
        if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"}
    )
    if not image_paths:
        raise SystemExit(f"No QR image files found in: {input_dir}")

    parts: dict[int, bytes] = {}
    manifest = load_manifest_json(input_dir)
    failures: list[str] = []

    for image_path in image_paths:
        text = decode_image(image_path)
        if not text:
            failures.append(f"{image_path.name}: no QR code decoded")
            continue

        if text.startswith(f"{MANIFEST_MAGIC}\n"):
            try:
                qr_manifest = parse_manifest_qr_text(text)
            except Exception as exc:
                failures.append(f"{image_path.name}: {exc}")
                continue
            if manifest is None:
                manifest = qr_manifest
            elif manifest != qr_manifest:
                failures.append(f"{image_path.name}: manifest QR does not match manifest.json")
            if args.verbose:
                print(f"Read {image_path.name} as manifest")
            continue

        try:
            metadata, payload = parse_qr_text(text)
        except Exception as exc:
            failures.append(f"{image_path.name}: {exc}")
            continue

        required_keys = {"part_index", "part_count", "payload_encoding"}
        missing = sorted(required_keys - set(metadata))
        if missing:
            failures.append(f"{image_path.name}: missing metadata keys {missing}")
            continue

        if metadata.get("payload_encoding") != "base85":
            failures.append(f"{image_path.name}: unsupported payload encoding {metadata.get('payload_encoding')!r}")
            continue

        if "chunk_sha256" in metadata and sha256_bytes(payload) != metadata["chunk_sha256"]:
            failures.append(f"{image_path.name}: chunk SHA-256 mismatch")
            continue

        if manifest is not None and int(metadata["part_count"]) != int(manifest["part_count"]):
            failures.append(f"{image_path.name}: part count does not match manifest")
            continue

        part_index = int(metadata["part_index"])
        if part_index in parts:
            failures.append(f"{image_path.name}: duplicate part {part_index}")
            continue
        parts[part_index] = payload

        if args.verbose:
            print(f"Read {image_path.name} as part {part_index}")

    if manifest is None:
        raise SystemExit("No manifest.json or valid manifest QR was found.")

    part_count = int(manifest["part_count"])
    missing_parts = [index for index in range(1, part_count + 1) if index not in parts]
    if missing_parts:
        preview = ", ".join(map(str, missing_parts[:20]))
        suffix = "..." if len(missing_parts) > 20 else ""
        details = f"Missing {len(missing_parts)} part(s): {preview}{suffix}"
        if failures:
            details += "\n\nImage issues:\n" + "\n".join(f"- {failure}" for failure in failures)
        raise SystemExit(details)

    output_path = (
        Path(args.output).expanduser().resolve()
        if args.output
        else input_dir.with_name(str(manifest["file_name"]))
    )
    if output_path.exists() and not args.force:
        raise SystemExit(f"Output already exists, use --force to overwrite: {output_path}")

    with output_path.open("wb") as output_file:
        for index in range(1, part_count + 1):
            output_file.write(parts[index])

    actual_size = output_path.stat().st_size
    actual_hash = sha256_file(output_path)
    if actual_size != int(manifest["file_size"]):
        output_path.unlink(missing_ok=True)
        raise SystemExit(
            f"Reconstructed size mismatch: expected {manifest['file_size']}, got {actual_size}"
        )
    if actual_hash != manifest["file_sha256"]:
        output_path.unlink(missing_ok=True)
        raise SystemExit(
            f"Reconstructed SHA-256 mismatch: expected {manifest['file_sha256']}, got {actual_hash}"
        )

    print(f"Reconstructed: {output_path}")
    print(f"Verified SHA-256: {actual_hash}")
    if failures:
        print("\nIgnored non-fatal image issues:")
        for failure in failures:
            print(f"- {failure}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Encode a file into multiple QR codes, or decode QR codes back into the file."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    encode = subparsers.add_parser("encode", help="Create QR images from a file.")
    encode.add_argument("input", help="Input file, for example .zip, .7z, .tar, .rar, or any binary file.")
    encode.add_argument("-o", "--output", help="Output folder. Defaults to <file_stem>_img beside the input.")
    encode.add_argument(
        "--chunk-bytes",
        type=int,
        default=DEFAULT_CHUNK_BYTES,
        help=f"Raw bytes per QR before Base85 encoding. Default: {DEFAULT_CHUNK_BYTES}.",
    )
    encode.add_argument(
        "--ecc",
        choices=ECC_LEVELS,
        default="Q",
        help="QR error correction level. Q is a balanced default; H is tougher but creates more/larger QRs.",
    )
    encode.add_argument("--box-size", type=int, default=DEFAULT_BOX_SIZE, help="Pixels per QR module.")
    encode.add_argument("--border", type=int, default=DEFAULT_BORDER, help="Quiet-zone border modules.")
    encode.add_argument(
        "--mask-pattern",
        type=int,
        choices=MASK_PATTERNS,
        default=DEFAULT_MASK_PATTERN,
        metavar="0-7",
        help="Preferred QR mask pattern. The encoder self-check may choose another pattern per part.",
    )
    encode.add_argument(
        "--no-self-check",
        action="store_true",
        help="Skip trying mask patterns against OpenCV during encoding.",
    )
    encode.add_argument("-v", "--verbose", action="store_true", help="Print every part as it is written.")
    encode.add_argument("-f", "--force", action="store_true", help="Replace generated QR files in an existing output folder.")
    encode.set_defaults(func=encode_file)

    decode = subparsers.add_parser("decode", help="Reconstruct a file from a QR image folder.")
    decode.add_argument("input", help="Folder containing QR images.")
    decode.add_argument("-o", "--output", help="Output file. Defaults to the original file name beside the folder.")
    decode.add_argument("-f", "--force", action="store_true", help="Overwrite the output file if it already exists.")
    decode.add_argument("-v", "--verbose", action="store_true", help="Print every part as it is read.")
    decode.set_defaults(func=decode_folder)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Cancelled.", file=sys.stderr)
        raise SystemExit(130)
