#!/usr/bin/env python3
"""
SwarmWatch Release Package Verifier (CLI).

Single-command in-process distribution verification for Python binary wheels (.whl)
and TypeScript npm tarballs (.tgz).
"""

import argparse
import json
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
PYTHON_SDK_PATH = ROOT_DIR / "packages" / "sdk-python"
sys.path.insert(0, str(PYTHON_SDK_PATH))

import importlib.util

validator_file = ROOT_DIR / 'packages' / 'sdk-python' / 'swarmwatch' / 'release_validator.py'
spec = importlib.util.spec_from_file_location('swarmwatch.release_validator', validator_file)
release_validator_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release_validator_mod)

PackageReleaseValidator = release_validator_mod.PackageReleaseValidator
FullDistributionReport = release_validator_mod.FullDistributionReport


def main():
    parser = argparse.ArgumentParser(
        description="SwarmWatch Distribution Package Integrity Verifier"
    )
    parser.add_argument(
        "--wheel",
        type=Path,
        default=None,
        help="Path to Python .whl package (auto-detected in dist/ if omitted)",
    )
    parser.add_argument(
        "--tarball",
        type=Path,
        default=None,
        help="Path to TypeScript .tgz package (auto-detected in packages/sdk-typescript/ if omitted)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="Output structured JSON report to stdout",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail if any warnings or anomalies are detected",
    )

    args = parser.parse_args()

    # Auto-detection
    wheel_path = args.wheel
    if wheel_path is None:
        dist_dir = ROOT_DIR / "packages" / "sdk-python" / "dist"
        if dist_dir.exists():
            wheels = list(dist_dir.glob("*.whl"))
            if wheels:
                wheel_path = wheels[0]

    tarball_path = args.tarball
    if tarball_path is None:
        ts_dir = ROOT_DIR / "packages" / "sdk-typescript"
        tarballs = list(ts_dir.glob("*.tgz"))
        if tarballs:
            tarball_path = tarballs[0]

    validator = PackageReleaseValidator()
    report: FullDistributionReport = validator.run_full_suite(
        wheel_path=wheel_path, tarball_path=tarball_path
    )

    if args.output_json:
        print(json.dumps(report.to_dict(), indent=2))
        sys.exit(0 if report.all_valid else 1)

    print("=" * 72)
    print(" SWARMWATCH DISTRIBUTION ARTIFACT VERIFICATION SUITE")
    print("=" * 72)

    for artifact in report.artifacts:
        status_symbol = "[PASS]" if artifact.is_valid else "[FAIL]"
        print(f"\n{status_symbol} {artifact.artifact_type.upper()}: {Path(artifact.artifact_path).name}")
        print(f"   Path:        {artifact.artifact_path}")
        print(f"   Package:     {artifact.package_name} v{artifact.version}")
        print(f"   Size:        {artifact.file_size_bytes:,} bytes")
        print(f"   SHA-256:     {artifact.sha256}")
        print(f"   Members:     {len(artifact.members)} files archived")

        if artifact.artifact_type == "wheel":
            print(f"   Dashboard:   {'Present (>1KB)' if artifact.has_dashboard else 'MISSING'}")
            print(f"   CLI Entry:   {', '.join(artifact.entry_points) if artifact.entry_points else 'None'}")

        if artifact.errors:
            print("   ERRORS:")
            for err in artifact.errors:
                print(f"     ! {err}")

        if artifact.warnings:
            print("   WARNINGS:")
            for warn in artifact.warnings:
                print(f"     ? {warn}")

    print("\n" + "-" * 72)
    print(f" RESULT: {report.summary}")
    print("=" * 72)

    sys.exit(0 if report.all_valid else 1)


if __name__ == "__main__":
    main()
