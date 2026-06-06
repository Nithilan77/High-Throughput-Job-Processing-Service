"""The actual unit of work: generate a thumbnail.

We try to fetch the source image, but fall back to a fast synthetic image if
the fetch fails. Either way the worker does real image work (decode/resize/
encode via Pillow), which is the representative cost we want to measure.
"""
import base64
import io
from PIL import Image


def _make_synthetic_image() -> Image.Image:
    """Fast synthetic source image.

    Uses Pillow's C-accelerated gradient/resize instead of a Python pixel
    loop, so the measured cost is the resize+encode (the real work), not
    synthetic-image construction.
    """
    base = Image.linear_gradient("L")          # 256x256 grayscale gradient (C)
    img = Image.merge("RGB", (base, base.rotate(90), base.rotate(180)))
    return img.resize((1024, 1024))            # realistic source size (C-level)


def generate_thumbnail(image_bytes: bytes | None, width: int, height: int) -> dict:
    """Pure function: bytes -> thumbnail metadata. Easy to unit test."""
    if image_bytes:
        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    else:
        img = _make_synthetic_image()

    original_size = img.size
    img.thumbnail((width, height))
    out = io.BytesIO()
    img.save(out, format="PNG")
    encoded = base64.b64encode(out.getvalue()).decode("ascii")
    return {
        "original_size": list(original_size),
        "thumbnail_size": list(img.size),
        "thumbnail_b64_len": len(encoded),
    }