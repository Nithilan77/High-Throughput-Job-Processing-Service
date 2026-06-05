"""The actual unit of work: generate a thumbnail.

We try to fetch the source image, but fall back to synthesizing one if the
fetch fails. Either way the worker does real CPU work (decode/resize/encode),
which keeps load tests free of external-network dependence.
"""
import base64
import io
from PIL import Image


def _make_synthetic_image(seed: int = 0) -> Image.Image:
    img = Image.new("RGB", (512, 512))
    px = img.load()
    for y in range(512):
        for x in range(0, 512, 4):
            v = (x + y + seed) % 256
            for dx in range(4):
                if x + dx < 512:
                    px[x + dx, y] = (v, (v * 2) % 256, (v * 3) % 256)
    return img


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
        # Store length, not the blob, to keep Redis lean; a real system would
        # push the blob to object storage and store a URL here.
    }