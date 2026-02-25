#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import math
import argparse
from dataclasses import dataclass
from typing import List, Dict, Tuple, Iterable, Optional

import numpy as np
import tifffile as tiff
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

try:
    from scipy import ndimage as ndi
except ImportError:
    raise SystemExit("scipy is required: pip install scipy")


# Config

@dataclass(frozen=True)
class Config:
    # Tiling
    tile_size: int = 2048
    overlap: int = 32

    # Detection thresholds
    nsigma: float = 5.0
    min_area: int = 4
    max_area: int = 200_000
    max_objects_per_tile: int = 20_000

    # Anti-false-positive guards
    min_sigma: float = 1.0
    min_peak_above_bkg: float = 5.0
    min_snr_peak: float = 5.0

    bkg_mode: str = "block"
    bkg_block: int = 64
    clip_high_percentile: float = 95.0
    percentile_sample_step: int = 8


# Utilities

def robust_sigma_mad(x: np.ndarray) -> float:
    #Robust sigma estimate via MAD.
    med = np.median(x)
    mad = np.median(np.abs(x - med))
    return 1.4826 * mad + 1e-12  # avoid division by zero


def iter_tiles(h: int, w: int, tile: int, overlap: int) -> Iterable[Tuple[int, int, int, int]]:
    #Tile generator: (y0, y1, x0, x1), with overlap.
    step = max(1, tile - overlap)
    for y0 in range(0, h, step):
        y1 = min(h, y0 + tile)
        for x0 in range(0, w, step):
            x1 = min(w, x0 + tile)
            yield y0, y1, x0, x1


def classify_object(area: int, ellipticity: float, peak: float, flux: float) -> str:
    if area < 40 and ellipticity < 0.35 and peak > 0 and flux > 0:
        return "star"
    if area > 120 or ellipticity > 0.55:
        return "extended"
    return "unknown"


def region_moments(coords_y: np.ndarray, coords_x: np.ndarray, weights: np.ndarray) -> Tuple[float, float, float]:
    """
    Ellipticity from normalized central second moments.
    Returns (ellipticity, major, minor) approximately.
    """
    wsum = np.sum(weights) + 1e-12
    x = coords_x.astype(np.float64)
    y = coords_y.astype(np.float64)
    wx = np.sum(weights * x) / wsum
    wy = np.sum(weights * y) / wsum

    dx = x - wx
    dy = y - wy

    mu20 = np.sum(weights * dx * dx) / wsum
    mu02 = np.sum(weights * dy * dy) / wsum
    mu11 = np.sum(weights * dx * dy) / wsum

    # Eigenvalues of the covariance matrix
    tr = mu20 + mu02
    det = mu20 * mu02 - mu11 * mu11
    disc = max(tr * tr - 4.0 * det, 0.0)
    l1 = 0.5 * (tr + math.sqrt(disc))
    l2 = 0.5 * (tr - math.sqrt(disc))

    major = math.sqrt(max(l1, 0.0))
    minor = math.sqrt(max(l2, 0.0))
    if major <= 1e-12:
        return 0.0, major, minor

    ellipticity = 1.0 - (minor / major)
    return float(ellipticity), float(major), float(minor)


# Background estimation

def _percentile_fast(arr: np.ndarray, q: float, step: int) -> float:
    flat = arr.ravel()
    if step <= 1 or flat.size < 10000:
        return float(np.percentile(flat, q))
    sample = flat[::step]
    return float(np.percentile(sample, q))


def background_block_median(tile: np.ndarray, block: int, clip_hi_q: float, sample_step: int) -> np.ndarray:
    """
    Fast background map via block-wise median (with reflect padding).
    Bright pixels are clipped to reduce object influence.
    """
    tile_f = tile.astype(np.float32, copy=False)

    hi = _percentile_fast(tile_f, clip_hi_q, sample_step)
    clipped = np.minimum(tile_f, hi)

    h, w = clipped.shape
    bh = (h + block - 1) // block
    bw = (w + block - 1) // block

    pad_h = bh * block - h
    pad_w = bw * block - w
    padded = np.pad(clipped, ((0, pad_h), (0, pad_w)), mode="reflect")

    reshaped = padded.reshape(bh, block, bw, block)
    coarse = np.median(reshaped, axis=(1, 3))  # (bh, bw)

    bkg = np.repeat(np.repeat(coarse, block, axis=0), block, axis=1)[:h, :w]
    return bkg.astype(np.float32, copy=False)


def estimate_bkg_and_sigma(tile: np.ndarray, cfg: Config) -> Tuple[float, float, Optional[np.ndarray]]:
    """
    Returns (bkg_scalar, sigma, bkg_map_or_none).
    Detection is performed on residual:
        residual = tile - bkg_map  (or tile - bkg_scalar)
    """
    if cfg.bkg_mode == "block":
        bkg_map = background_block_median(
            tile=tile,
            block=cfg.bkg_block,
            clip_hi_q=cfg.clip_high_percentile,
            sample_step=cfg.percentile_sample_step,
        )
        resid = tile.astype(np.float32, copy=False) - bkg_map

        # Clip bright residual tail to avoid stars inflating sigma
        hi_r = _percentile_fast(resid, 95.0, cfg.percentile_sample_step)
        core = resid[resid <= hi_r]
        if core.size < 100:
            core = resid

        sig = float(robust_sigma_mad(core))
        sig = max(sig, cfg.min_sigma)

        bkg_scalar = float(np.median(bkg_map))
        return bkg_scalar, sig, bkg_map

    # Simple scalar background (fallback)
    tile_f = tile.astype(np.float32, copy=False)
    hi = _percentile_fast(tile_f, cfg.clip_high_percentile, cfg.percentile_sample_step)
    core = tile_f[tile_f <= hi]
    if core.size < 100:
        core = tile_f

    bkg = float(np.median(core))
    sig_mad = float(robust_sigma_mad(core))

    p16 = _percentile_fast(core, 16.0, cfg.percentile_sample_step)
    p84 = _percentile_fast(core, 84.0, cfg.percentile_sample_step)
    sig_pct = float((p84 - p16) / 2.0)

    sig = max(sig_mad, sig_pct, cfg.min_sigma)
    return bkg, sig, None


# Single-tile analysis

def analyze_tile(tile_img: np.ndarray, cfg: Config) -> Tuple[List[Dict], Dict]:
    """
    Analyze one tile and return (objects, tile_stats).
    Detection is performed on residual = tile - background.
    """
    bkg_scalar, sig, bkg_map = estimate_bkg_and_sigma(tile_img, cfg)

    if bkg_map is None:
        resid = tile_img.astype(np.float32, copy=False) - bkg_scalar
    else:
        resid = tile_img.astype(np.float32, copy=False) - bkg_map

    thr = cfg.nsigma * sig
    mask = resid > thr

    if not np.any(mask):
        return [], {"bkg": bkg_scalar, "sigma": sig, "thr": thr, "n": 0}

    labels, nlab = ndi.label(mask)
    if nlab == 0:
        return [], {"bkg": bkg_scalar, "sigma": sig, "thr": thr, "n": 0}

    # Fast area estimate
    counts = np.bincount(labels.ravel())
    if nlab > cfg.max_objects_per_tile:
        return [], {"bkg": bkg_scalar, "sigma": sig, "thr": thr, "n": 0, "note": "too_many_objects"}

    objects: List[Dict] = []
    slices = ndi.find_objects(labels)

    for lab, slc in enumerate(slices, start=1):
        if slc is None:
            continue

        area = int(counts[lab]) if lab < len(counts) else 0
        if area < cfg.min_area or area > cfg.max_area:
            continue

        sub_labels = labels[slc]
        sub_resid = resid[slc]

        m = (sub_labels == lab)
        if not np.any(m):
            continue

        # Residual pixels for this object (already background-subtracted)
        obj_res = sub_resid[m].astype(np.float32, copy=False)

        peak_above = float(obj_res.max())
        if peak_above < cfg.min_peak_above_bkg:
            continue

        snr_peak = peak_above / sig
        if snr_peak < cfg.min_snr_peak:
            continue

        # Flux is the sum of background-subtracted positive signal
        obj_res_pos = np.clip(obj_res, 0, None)
        flux = float(obj_res_pos.sum())
        if flux <= 0:
            continue

        # Object pixel coordinates within the tile
        ys_local, xs_local = np.nonzero(m)
        ys = ys_local + (slc[0].start or 0)
        xs = xs_local + (slc[1].start or 0)

        # Intensity-weighted centroid and shape moments computed on residual weights
        w = np.clip(obj_res_pos.astype(np.float64, copy=False), 0, None)
        wsum = float(np.sum(w)) + 1e-12
        cx = float(np.sum(xs * w) / wsum)
        cy = float(np.sum(ys * w) / wsum)

        ellipticity, major, minor = region_moments(ys, xs, w)
        obj_type = classify_object(area, ellipticity, peak_above, flux)

        objects.append({
            "x": cx,
            "y": cy,
            "flux": flux,
            "peak": peak_above,  # peak is residual peak (peak - background)
            "area": area,
            "ellipticity": ellipticity,
            "major": major,
            "minor": minor,
            "type": obj_type,
        })

    return objects, {"bkg": bkg_scalar, "sigma": sig, "thr": thr, "n": len(objects)}


# TIFF reading

def read_tiff(path: str) -> np.ndarray:
    """
    Read a TIFF into a 2D numpy array (grayscale float32).

    Handles:
      - 2D grayscale: (H, W)
      - multi-page:   (pages, H, W) -> first page
      - RGB/RGBA:     (H, W, C) -> grayscale
      - planar RGB:   (C, H, W) -> grayscale
      - multi-page RGB: (pages, H, W, C) -> first page -> grayscale
    """
    img = tiff.imread(path)

    # Multi-page RGB: (pages, H, W, C)
    if img.ndim == 4 and img.shape[-1] in (3, 4):
        img = img[0]

    # Multi-page grayscale: (pages, H, W)
    if img.ndim == 3 and img.shape[-1] not in (3, 4) and img.shape[0] > 1 and img.shape[1] > 16 and img.shape[2] > 16:
        img = img[0]

    # Planar RGB: (C, H, W)
    if img.ndim == 3 and img.shape[0] in (3, 4) and img.shape[1] > 16 and img.shape[2] > 16:
        img = np.moveaxis(img, 0, -1)

    # RGB/RGBA: (H, W, C) -> grayscale
    if img.ndim == 3 and img.shape[-1] in (3, 4):
        rgb = img[..., :3].astype(np.float32, copy=False)
        img = rgb.mean(axis=-1)

    if img.ndim != 2:
        raise ValueError(f"Expected a 2D image after conversion, got shape={img.shape} for {path}")

    return img.astype(np.float32, copy=False)


# Single file analysis

def analyze_image_file(path: str, cfg: Config) -> Tuple[str, List[Dict], Dict]:
    # Process one file: tile -> analyze -> collect objects with global coordinates.
    img = read_tiff(path)
    h, w = img.shape

    all_objects: List[Dict] = []
    tile_stats = []

    for (y0, y1, x0, x1) in iter_tiles(h, w, cfg.tile_size, cfg.overlap):
        tile = img[y0:y1, x0:x1]
        objs, st = analyze_tile(tile, cfg)
        st.update({"y0": y0, "y1": y1, "x0": x0, "x1": x1})
        tile_stats.append(st)

        # Convert tile coordinates to image coordinates
        for o in objs:
            o["x"] = float(o["x"] + x0)
            o["y"] = float(o["y"] + y0)
            all_objects.append(o)

    image_stats = {
        "file": os.path.basename(path),
        "h": h,
        "w": w,
        "tiles": len(tile_stats),
        "objects": len(all_objects),
        "bkg_mode": cfg.bkg_mode,
        "median_bkg_mean": float(np.mean([s["bkg"] for s in tile_stats])) if tile_stats else 0.0,
        "sigma_mean": float(np.mean([s["sigma"] for s in tile_stats])) if tile_stats else 0.0,
    }

    return os.path.basename(path), all_objects, image_stats


# Parallel run + CSV writing

def find_tiff_files(input_dir: str) -> List[str]:
    exts = (".tif", ".tiff", ".TIF", ".TIFF")
    paths: List[str] = []
    for root, _, files in os.walk(input_dir):
        for fn in files:
            if fn.endswith(exts):
                paths.append(os.path.join(root, fn))
    paths.sort()
    return paths


def main():
    ap = argparse.ArgumentParser(description="Parallel TIFF astro object analysis (tiles + stats)")

    ap.add_argument("--input", required=True, help="Folder containing .tif/.tiff files")
    ap.add_argument("--out", default="objects.csv", help="CSV output for detected objects")
    ap.add_argument("--out-images", default="images_stats.csv", help="CSV output with per-image statistics")
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1), help="Number of worker processes")

    ap.add_argument("--tile", type=int, default=2048, help="Tile size")
    ap.add_argument("--overlap", type=int, default=32, help="Tile overlap")

    ap.add_argument("--nsigma", type=float, default=5.0, help="Detection threshold on residual: nsigma * sigma")
    ap.add_argument("--min-area", type=int, default=4, help="Minimum object area (pixels)")
    ap.add_argument("--max-area", type=int, default=200000, help="Maximum object area (pixels)")

    ap.add_argument("--min-sigma", type=float, default=1.0, help="Sigma floor (useful for mostly-flat tiles)")
    ap.add_argument("--min-peak-above-bkg", type=float, default=5.0, help="Minimum residual peak (peak - background)")
    ap.add_argument("--min-snr-peak", type=float, default=5.0, help="Minimum peak SNR: (peak - bkg) / sigma")

    ap.add_argument("--bkg-mode", choices=["block", "simple"], default="block",
                    help="Background mode: block (fast map) or simple (scalar per tile)")
    ap.add_argument("--bkg-block", type=int, default=64, help="Block size for block-median background (pixels)")
    ap.add_argument("--clip-high-percentile", type=float, default=95.0, help="Clip bright tail when estimating background")
    ap.add_argument("--percentile-sample-step", type=int, default=8, help="Subsampling step for percentiles (bigger = faster)")

    args = ap.parse_args()

    cfg = Config(
        tile_size=args.tile,
        overlap=args.overlap,
        nsigma=args.nsigma,
        min_area=args.min_area,
        max_area=args.max_area,
        min_sigma=args.min_sigma,
        min_peak_above_bkg=args.min_peak_above_bkg,
        min_snr_peak=args.min_snr_peak,
        bkg_mode=args.bkg_mode,
        bkg_block=args.bkg_block,
        clip_high_percentile=args.clip_high_percentile,
        percentile_sample_step=args.percentile_sample_step,
    )

    files = find_tiff_files(args.input)
    if not files:
        raise SystemExit("No TIFF files found.")

    obj_fields = ["file", "obj_id", "x", "y", "flux", "peak", "area", "ellipticity", "major", "minor", "type"]
    img_fields = ["file", "h", "w", "tiles", "objects", "bkg_mode", "median_bkg_mean", "sigma_mean"]

    with open(args.out, "w", newline="", encoding="utf-8") as fobj, \
         open(args.out_images, "w", newline="", encoding="utf-8") as fimg:

        obj_writer = csv.DictWriter(fobj, fieldnames=obj_fields)
        img_writer = csv.DictWriter(fimg, fieldnames=img_fields)
        obj_writer.writeheader()
        img_writer.writeheader()

        with ProcessPoolExecutor(max_workers=args.workers) as ex:
            futures = {ex.submit(analyze_image_file, p, cfg): p for p in files}

            for fut in tqdm(as_completed(futures), total=len(futures), desc="Processing"):
                p = futures[fut]
                try:
                    fname, objects, img_stat = fut.result()
                except Exception as e:
                    tqdm.write(f"[ERROR] {os.path.basename(p)}: {e}")
                    continue

                img_writer.writerow(img_stat)

                for i, o in enumerate(objects):
                    obj_writer.writerow({"file": fname, "obj_id": i, **o})

    print(f"Done.\n- Objects: {args.out}\n- Per-image stats: {args.out_images}")


if __name__ == "__main__":
    main()