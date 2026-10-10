import io
import os
import tarfile
import zipfile
from pathlib import Path
import pytest

from swarmwatch.release_validator import (
    PackageReleaseValidator,
    ArtifactIntegrityReport,
    FullDistributionReport,
)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
PYTHON_DIST = ROOT_DIR / "packages" / "sdk-python" / "dist"
WHEEL_PATH = next(PYTHON_DIST.glob("*.whl"), None)
TS_DIST = ROOT_DIR / "packages" / "sdk-typescript" / "dist"
TS_TARBALL_PATH = next(TS_DIST.glob("*.tgz"), ROOT_DIR / "packages" / "sdk-typescript" / "swarmwatch-sdk-0.1.0.tgz")


def test_wheel_validation_real_package():
    assert WHEEL_PATH is not None and WHEEL_PATH.exists(), f"Wheel not found at {WHEEL_PATH}"
    validator = PackageReleaseValidator()
    report = validator.validate_wheel(WHEEL_PATH)

    assert isinstance(report, ArtifactIntegrityReport)
    assert report.is_valid is True, f"Validation failed: {report.errors}"
    assert report.artifact_type == "wheel"
    assert report.package_name == "swarmwatch"
    assert report.version == "0.1.0"
    assert report.has_dashboard is True
    assert "swarmwatch = swarmwatch.cli:main" in report.entry_points
    assert len(report.sha256) == 64
    assert report.file_size_bytes > 50000


def test_wheel_validation_detects_corrupted_file(tmp_path):
    bad_file = tmp_path / "corrupt.whl"
    bad_file.write_bytes(b"NOT_A_ZIP_HEADER_JUNK_DATA")

    validator = PackageReleaseValidator()
    report = validator.validate_wheel(bad_file)

    assert report.is_valid is False
    assert any("not a valid zip" in err.lower() for err in report.errors)


def test_wheel_validation_detects_missing_dashboard(tmp_path):
    mock_wheel = tmp_path / "incomplete-0.1.0-py3-none-any.whl"
    with zipfile.ZipFile(mock_wheel, "w") as zf:
        zf.writestr("swarmwatch/__init__.py", "__version__ = '0.1.0'")
        zf.writestr("swarmwatch-0.1.0.dist-info/METADATA", "Metadata-Version: 2.1\nName: swarmwatch\nVersion: 0.1.0\n")
        zf.writestr("swarmwatch-0.1.0.dist-info/entry_points.txt", "[console_scripts]\nswarmwatch = swarmwatch.cli:main\n")

    validator = PackageReleaseValidator()
    report = validator.validate_wheel(mock_wheel)

    assert report.is_valid is False
    assert any("dashboard.html" in err for err in report.errors)


def test_npm_tarball_validation_real_package():
    assert TS_TARBALL_PATH.exists(), f"Tarball not found at {TS_TARBALL_PATH}"
    validator = PackageReleaseValidator()
    report = validator.validate_npm_tarball(TS_TARBALL_PATH)

    assert isinstance(report, ArtifactIntegrityReport)
    assert report.is_valid is True, f"Tarball validation failed: {report.errors}"
    assert report.artifact_type == "npm_tarball"
    assert report.package_name == "@swarmwatch/sdk"
    assert report.version == "0.1.0"
    assert len(report.members) >= 8
    assert "package/src/index.ts" in report.members or "package/package.json" in report.members
    assert len(report.sha256) == 64
    assert report.file_size_bytes > 5000


def test_npm_tarball_validation_detects_corrupted_file(tmp_path):
    bad_file = tmp_path / "corrupt.tgz"
    bad_file.write_bytes(b"NOT_A_GZIP_OR_TAR_DATA")

    validator = PackageReleaseValidator()
    report = validator.validate_npm_tarball(bad_file)

    assert report.is_valid is False
    assert any("not a valid" in err.lower() for err in report.errors)


def test_full_suite_validation():
    validator = PackageReleaseValidator()
    full_report = validator.run_full_suite(WHEEL_PATH, TS_TARBALL_PATH)

    assert isinstance(full_report, FullDistributionReport)
    assert full_report.all_valid is True
    assert len(full_report.artifacts) == 2
    assert full_report.total_errors == 0
