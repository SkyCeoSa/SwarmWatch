"""
SwarmWatch Distribution Package Validator (Deep Module).

Provides in-process, zero-cloud verification of Python binary wheels (.whl)
and TypeScript npm tarballs (.tgz) with structural and mathematical integrity checks.
"""

from dataclasses import dataclass, field, asdict
import hashlib
import json
from pathlib import Path
import tarfile
from typing import List, Optional, Union
import zipfile


@dataclass
class ArtifactIntegrityReport:
    artifact_path: str
    artifact_type: str  # "wheel" or "npm_tarball"
    is_valid: bool
    package_name: str = ""
    version: str = ""
    file_size_bytes: int = 0
    sha256: str = ""
    members: List[str] = field(default_factory=list)
    has_dashboard: bool = False
    entry_points: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class FullDistributionReport:
    all_valid: bool
    artifacts: List[ArtifactIntegrityReport] = field(default_factory=list)
    total_errors: int = 0
    summary: str = ""

    def to_dict(self) -> dict:
        return {
            "all_valid": self.all_valid,
            "total_errors": self.total_errors,
            "summary": self.summary,
            "artifacts": [a.to_dict() for a in self.artifacts],
        }


class PackageReleaseValidator:
    """
    Deep module encapsulating archive parsing, metadata validation,
    and asset verification behind a small, cohesive interface.
    """

    FORBIDDEN_PATTERNS = [
        "__pycache__",
        ".pyc",
        ".env",
        "id_rsa",
        ".git",
        "node_modules",
    ]

    def _compute_sha256(self, path: Path) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(65536):
                h.update(chunk)
        return h.hexdigest()

    def validate_wheel(self, wheel_path: Union[str, Path]) -> ArtifactIntegrityReport:
        p = Path(wheel_path)
        report = ArtifactIntegrityReport(
            artifact_path=str(p),
            artifact_type="wheel",
            is_valid=True,
        )

        if not p.exists():
            report.is_valid = False
            report.errors.append(f"Wheel package not found at {p}")
            return report

        report.file_size_bytes = p.stat().st_size
        report.sha256 = self._compute_sha256(p)

        if not zipfile.is_zipfile(p):
            report.is_valid = False
            report.errors.append(f"Not a valid zip archive: {p.name}")
            return report

        try:
            with zipfile.ZipFile(p, "r") as zf:
                # Test archive integrity
                corrupt_file = zf.testzip()
                if corrupt_file:
                    report.is_valid = False
                    report.errors.append(f"Corrupted zip member: {corrupt_file}")
                    return report

                namelist = zf.namelist()
                report.members = namelist

                # Check for dashboard.html
                dashboard_members = [
                    m for m in namelist if m.endswith("dashboard.html")
                ]
                if dashboard_members:
                    dash_info = zf.getinfo(dashboard_members[0])
                    if dash_info.file_size > 1000:
                        report.has_dashboard = True
                    else:
                        report.is_valid = False
                        report.errors.append(
                            f"Embedded dashboard.html is too small ({dash_info.file_size} bytes)"
                        )
                else:
                    report.is_valid = False
                    report.errors.append(
                        "Missing mandatory embedded asset: swarmwatch/dashboard.html"
                    )

                # Check metadata and entry points in .dist-info
                metadata_members = [
                    m for m in namelist if m.endswith(".dist-info/METADATA")
                ]
                if metadata_members:
                    metadata_content = zf.read(metadata_members[0]).decode(
                        "utf-8", errors="ignore"
                    )
                    for line in metadata_content.splitlines():
                        if line.startswith("Name:"):
                            report.package_name = line.split(":", 1)[1].strip()
                        elif line.startswith("Version:"):
                            report.version = line.split(":", 1)[1].strip()
                else:
                    report.is_valid = False
                    report.errors.append("Missing .dist-info/METADATA in wheel")

                entry_points_members = [
                    m
                    for m in namelist
                    if m.endswith(".dist-info/entry_points.txt")
                ]
                if entry_points_members:
                    ep_content = zf.read(entry_points_members[0]).decode(
                        "utf-8", errors="ignore"
                    )
                    report.entry_points = [
                        line.strip()
                        for line in ep_content.splitlines()
                        if line.strip() and not line.startswith("[")
                    ]

                # Check for forbidden files
                for m in namelist:
                    for pattern in self.FORBIDDEN_PATTERNS:
                        if pattern in m:
                            report.is_valid = False
                            report.errors.append(
                                f"Forbidden member found in package: {m} (matches {pattern})"
                            )

        except Exception as e:
            report.is_valid = False
            report.errors.append(f"Unexpected error inspecting wheel: {str(e)}")

        return report

    def validate_npm_tarball(
        self, tarball_path: Union[str, Path]
    ) -> ArtifactIntegrityReport:
        p = Path(tarball_path)
        report = ArtifactIntegrityReport(
            artifact_path=str(p),
            artifact_type="npm_tarball",
            is_valid=True,
        )

        if not p.exists():
            report.is_valid = False
            report.errors.append(f"Tarball package not found at {p}")
            return report

        report.file_size_bytes = p.stat().st_size
        report.sha256 = self._compute_sha256(p)

        try:
            with tarfile.open(p, "r:*") as tf:
                members = tf.getmembers()
                namelist = [m.name for m in members]
                report.members = namelist

                # Find package.json
                pkg_json_member = next(
                    (m for m in members if m.name.endswith("package.json")),
                    None,
                )
                if pkg_json_member:
                    f = tf.extractfile(pkg_json_member)
                    if f:
                        data = json.loads(f.read().decode("utf-8"))
                        report.package_name = data.get("name", "")
                        report.version = data.get("version", "")
                else:
                    report.is_valid = False
                    report.errors.append("Missing package.json in npm tarball")

                # Check for core source files
                has_index = any(
                    "src/index.ts" in n or "dist/index.js" in n for n in namelist
                )
                if not has_index:
                    report.is_valid = False
                    report.errors.append(
                        "Missing core index module (src/index.ts or dist/index.js)"
                    )

                # Check for forbidden files
                for m in namelist:
                    for pattern in self.FORBIDDEN_PATTERNS:
                        if pattern in m:
                            report.is_valid = False
                            report.errors.append(
                                f"Forbidden member found in tarball: {m} (matches {pattern})"
                            )

        except Exception as e:
            report.is_valid = False
            report.errors.append(
                f"Not a valid tarball archive ({p.name}): {str(e)}"
            )

        return report

    def run_full_suite(
        self,
        wheel_path: Optional[Union[str, Path]] = None,
        tarball_path: Optional[Union[str, Path]] = None,
    ) -> FullDistributionReport:
        artifacts: List[ArtifactIntegrityReport] = []
        if wheel_path:
            artifacts.append(self.validate_wheel(wheel_path))
        if tarball_path:
            artifacts.append(self.validate_npm_tarball(tarball_path))

        all_valid = all(a.is_valid for a in artifacts) if artifacts else False
        total_errors = sum(len(a.errors) for a in artifacts)
        summary = (
            f"Verified {len(artifacts)} distribution artifact(s): "
            + ("ALL PASSED (100% Valid)" if all_valid else f"FAILED ({total_errors} error(s))")
        )

        return FullDistributionReport(
            all_valid=all_valid,
            artifacts=artifacts,
            total_errors=total_errors,
            summary=summary,
        )
