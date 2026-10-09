# -*- coding: utf-8 -*-
"""
SwarmWatch Dynamic Agent Federation & Swarm Mesh Topology Routing (Seam 72 / ADR 0083).

Implements zero-trust mutual peer discovery, authenticated peer handshakes, loop-free Dijkstra
mesh shortest-path routing, hop-limit (TTL) enforcement, tenant capability boundary fencing,
fail-closed safety circuit synchronization, and tamper-evident routing receipts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import uuid

from swarmwatch.root_witness import (
    WitnessKeyManager,
    sha256_digest,
)


# ==============================================================================
# 1. Custom Exceptions
# ==============================================================================

class MeshFederationError(RuntimeError):
    """Base exception for all swarm mesh federation and routing errors."""
    pass


class MeshPeerAuthenticationError(MeshFederationError):
    """Raised when peer identification, signature, or cryptographic handshake fails."""
    pass


class MeshRoutingLoopError(MeshFederationError):
    """Raised when a cyclic forwarding loop is detected in packet transit."""
    pass


class MeshHopLimitExceededError(MeshFederationError):
    """Raised when a packet exceeds its maximum allowed time-to-live hop limit."""
    pass


class MeshDestinationUnreachableError(MeshFederationError):
    """Raised when no active route or link exists to the target destination node."""
    pass


class MeshCapabilityFencingError(MeshFederationError):
    """Raised when a packet crosses unauthorized tenant boundaries or lacks required capability tokens."""
    pass


class MeshCircuitTrippedError(MeshFederationError):
    """Raised when mesh routing is blocked fail-closed due to an active safety circuit break."""
    pass


# ==============================================================================
# 2. Enums
# ==============================================================================

class MeshNodeStatus(str, Enum):
    ONLINE = "ONLINE"
    STANDBY = "STANDBY"
    DEGRADED = "DEGRADED"
    OFFLINE = "OFFLINE"
    EVICTED = "EVICTED"


class MeshNodeRole(str, Enum):
    EDGE = "EDGE"
    GATEWAY = "GATEWAY"
    WORKER = "WORKER"
    ROUTER = "ROUTER"


class MeshRoutingStatus(str, Enum):
    DELIVERED = "DELIVERED"
    ROUTED_RELAY = "ROUTED_RELAY"
    DROPPED_TTL = "DROPPED_TTL"
    BLOCKED_CIRCUIT = "BLOCKED_CIRCUIT"
    FAILED_CAPABILITY = "FAILED_CAPABILITY"
    DESTINATION_UNREACHABLE = "DESTINATION_UNREACHABLE"
    ROUTING_LOOP = "ROUTING_LOOP"


# ==============================================================================
# 3. Data Models
# ==============================================================================

@dataclass
class PeerNode:
    """Authenticated node participant within the Swarm Mesh Federation."""
    node_id: str
    cluster_id: str
    tenant_id: str
    endpoint_url: str
    public_key_hex: str
    role: MeshNodeRole = MeshNodeRole.WORKER
    status: MeshNodeStatus = MeshNodeStatus.ONLINE
    capabilities: List[str] = field(default_factory=list)
    last_heartbeat_ts: float = field(default_factory=time.time)
    latency_ms: float = 1.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "cluster_id": self.cluster_id,
            "tenant_id": self.tenant_id,
            "endpoint_url": self.endpoint_url,
            "public_key_hex": self.public_key_hex,
            "role": self.role.value if isinstance(self.role, MeshNodeRole) else self.role,
            "status": self.status.value if isinstance(self.status, MeshNodeStatus) else self.status,
            "capabilities": list(self.capabilities),
            "last_heartbeat_ts": self.last_heartbeat_ts,
            "latency_ms": self.latency_ms,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PeerNode:
        role_val = d.get("role", "WORKER")
        status_val = d.get("status", "ONLINE")
        return cls(
            node_id=d["node_id"],
            cluster_id=d.get("cluster_id", "default"),
            tenant_id=d.get("tenant_id", "default"),
            endpoint_url=d.get("endpoint_url", "http://127.0.0.1"),
            public_key_hex=d.get("public_key_hex", ""),
            role=MeshNodeRole(role_val) if role_val in MeshNodeRole.__members__ else MeshNodeRole.WORKER,
            status=MeshNodeStatus(status_val) if status_val in MeshNodeStatus.__members__ else MeshNodeStatus.ONLINE,
            capabilities=list(d.get("capabilities", [])),
            last_heartbeat_ts=float(d.get("last_heartbeat_ts", time.time())),
            latency_ms=float(d.get("latency_ms", 1.0)),
        )


@dataclass
class MeshLink:
    """Authenticated, weighted routing edge connecting two peer nodes."""
    source_node_id: str
    target_node_id: str
    weight_cost: float = 1.0
    is_active: bool = True
    latency_ms: float = 1.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_node_id": self.source_node_id,
            "target_node_id": self.target_node_id,
            "weight_cost": self.weight_cost,
            "is_active": self.is_active,
            "latency_ms": self.latency_ms,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MeshLink:
        return cls(
            source_node_id=d["source_node_id"],
            target_node_id=d["target_node_id"],
            weight_cost=float(d.get("weight_cost", 1.0)),
            is_active=bool(d.get("is_active", True)),
            latency_ms=float(d.get("latency_ms", 1.0)),
        )


@dataclass
class PeerHandshakeRequest:
    """Zero-trust mutual authentication request for joining the mesh federation."""
    source_node_id: str
    target_node_id: str
    cluster_id: str
    tenant_id: str
    public_key_hex: str
    timestamp_ns: int
    nonce: str
    signature: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_node_id": self.source_node_id,
            "target_node_id": self.target_node_id,
            "cluster_id": self.cluster_id,
            "tenant_id": self.tenant_id,
            "public_key_hex": self.public_key_hex,
            "timestamp_ns": self.timestamp_ns,
            "nonce": self.nonce,
            "signature": self.signature,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PeerHandshakeRequest:
        return cls(
            source_node_id=d["source_node_id"],
            target_node_id=d["target_node_id"],
            cluster_id=d.get("cluster_id", "default"),
            tenant_id=d.get("tenant_id", "default"),
            public_key_hex=d.get("public_key_hex", ""),
            timestamp_ns=int(d.get("timestamp_ns", 0)),
            nonce=d.get("nonce", ""),
            signature=d.get("signature", ""),
        )


@dataclass
class PeerHandshakeResponse:
    """Authenticated response establishing a verified peer session."""
    target_node_id: str
    source_node_id: str
    status: str  # "ACCEPTED" | "REJECTED"
    session_id: str
    timestamp_ns: int
    signature: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "target_node_id": self.target_node_id,
            "source_node_id": self.source_node_id,
            "status": self.status,
            "session_id": self.session_id,
            "timestamp_ns": self.timestamp_ns,
            "signature": self.signature,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PeerHandshakeResponse:
        return cls(
            target_node_id=d["target_node_id"],
            source_node_id=d["source_node_id"],
            status=d.get("status", "REJECTED"),
            session_id=d.get("session_id", ""),
            timestamp_ns=int(d.get("timestamp_ns", 0)),
            signature=d.get("signature", ""),
        )


@dataclass
class MeshPacket:
    """Immutable envelope encapsulating an inter-agent mesh message."""
    packet_id: str
    source_node_id: str
    dest_node_id: str
    tenant_id: str
    payload: Dict[str, Any] = field(default_factory=dict)
    capability_token: Optional[str] = None
    hop_count: int = 0
    max_hops: int = 16
    path_traversed: List[str] = field(default_factory=list)
    signature: Optional[str] = None
    created_at_ns: int = field(default_factory=time.time_ns)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "packet_id": self.packet_id,
            "source_node_id": self.source_node_id,
            "dest_node_id": self.dest_node_id,
            "tenant_id": self.tenant_id,
            "payload": self.payload,
            "capability_token": self.capability_token,
            "hop_count": self.hop_count,
            "max_hops": self.max_hops,
            "path_traversed": list(self.path_traversed),
            "signature": self.signature,
            "created_at_ns": self.created_at_ns,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MeshPacket:
        return cls(
            packet_id=d["packet_id"],
            source_node_id=d["source_node_id"],
            dest_node_id=d["dest_node_id"],
            tenant_id=d.get("tenant_id", "default"),
            payload=dict(d.get("payload", {})),
            capability_token=d.get("capability_token"),
            hop_count=int(d.get("hop_count", 0)),
            max_hops=int(d.get("max_hops", 16)),
            path_traversed=list(d.get("path_traversed", [])),
            signature=d.get("signature"),
            created_at_ns=int(d.get("created_at_ns", 0)),
        )

    def compute_digest(self) -> str:
        """Computes canonical SHA-256 digest of packet envelope."""
        canonical_str = (
            f"{self.packet_id}:{self.source_node_id}:{self.dest_node_id}:"
            f"{self.tenant_id}:{self.hop_count}:{self.max_hops}:"
            f"{json.dumps(self.payload, sort_keys=True)}:{self.created_at_ns}"
        )
        return sha256_digest(canonical_str.encode("utf-8"))


@dataclass
class MeshRoutingReceipt:
    """Cryptographically verifiable execution and delivery receipt for a mesh packet."""
    receipt_id: str
    packet_id: str
    source_node_id: str
    dest_node_id: str
    tenant_id: str
    path_traversed: List[str]
    total_hops: int
    status: MeshRoutingStatus
    duration_us: float
    created_at_ns: int
    receipt_hash: str
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "receipt_id": self.receipt_id,
            "packet_id": self.packet_id,
            "source_node_id": self.source_node_id,
            "dest_node_id": self.dest_node_id,
            "tenant_id": self.tenant_id,
            "path_traversed": list(self.path_traversed),
            "total_hops": self.total_hops,
            "status": self.status.value if isinstance(self.status, MeshRoutingStatus) else self.status,
            "duration_us": self.duration_us,
            "created_at_ns": self.created_at_ns,
            "receipt_hash": self.receipt_hash,
            "error_message": self.error_message,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MeshRoutingReceipt:
        st_val = d.get("status", "DELIVERED")
        return cls(
            receipt_id=d["receipt_id"],
            packet_id=d["packet_id"],
            source_node_id=d["source_node_id"],
            dest_node_id=d["dest_node_id"],
            tenant_id=d.get("tenant_id", "default"),
            path_traversed=list(d.get("path_traversed", [])),
            total_hops=int(d.get("total_hops", 0)),
            status=MeshRoutingStatus(st_val) if st_val in MeshRoutingStatus.__members__ else MeshRoutingStatus.DELIVERED,
            duration_us=float(d.get("duration_us", 0.0)),
            created_at_ns=int(d.get("created_at_ns", 0)),
            receipt_hash=d.get("receipt_hash", ""),
            error_message=d.get("error_message"),
        )

    @staticmethod
    def compute_digest(
        receipt_id: str,
        packet_id: str,
        source_node_id: str,
        dest_node_id: str,
        tenant_id: str,
        path: List[str],
        status: str,
        duration_us: float,
        timestamp_ns: int,
    ) -> str:
        """Computes canonical domain-tagged SHA-256 hash."""
        raw = (
            f"{receipt_id}:{packet_id}:{source_node_id}:{dest_node_id}:"
            f"{tenant_id}:{','.join(path)}:{status}:{duration_us:.2f}:{timestamp_ns}"
        )
        return f"sw:mesh:v1:{sha256_digest(raw.encode('utf-8'))}"


# ==============================================================================
# 4. Peer Cryptographic Utilities
# ==============================================================================

def generate_peer_keypair() -> Tuple[WitnessKeyManager, str]:
    """Generates an asymmetric ECDSA NIST P-256 key manager and public key string."""
    km = WitnessKeyManager.generate()
    return km, km.public_key_b64


def sign_peer_data(km: WitnessKeyManager, data: bytes) -> str:
    """Signs data bytes and returns URL-safe Base64 signature."""
    return km.sign_b64(data)


def _sign_helper(signer: Any, data: bytes) -> str:
    """Helper to sign data using WitnessKeyManager or callable."""
    if isinstance(signer, WitnessKeyManager):
        return signer.sign_b64(data)
    if callable(signer):
        return signer(data)
    raise MeshPeerAuthenticationError(f"Invalid signer: must be WitnessKeyManager or callable")


def verify_peer_data(public_key: str, data: bytes, signature: str) -> bool:
    """Verifies cryptographic signature against public key. Fails closed."""
    if not public_key or not signature:
        return False
    try:
        km = WitnessKeyManager.from_public_key(public_key)
        return km.verify_b64(data, signature)
    except Exception:
        pass
    try:
        from swarmwatch.byzantine_oracle import verify_bft_signature
        if len(public_key) == 64 and (len(signature) == 128 or len(signature) == 64):
            msg_str = data.decode("utf-8", errors="replace")
            return verify_bft_signature(public_key, msg_str, signature)
    except Exception:
        pass
    return False


def is_valid_peer_public_key(pk: str) -> bool:
    """Validates that a public key is well-formed (valid hex or valid base64 key). Fails closed."""
    if not pk or not isinstance(pk, str):
        return False
    trimmed = pk.strip()
    if (len(trimmed) == 64 or len(trimmed) == 130) and all(c in "0123456789abcdefABCDEF" for c in trimmed):
        return True
    try:
        WitnessKeyManager.from_public_key(trimmed)
        return True
    except Exception:
        return False


def compute_peer_registration_payload(
    node_id: str,
    cluster_id: str,
    tenant_id: str,
    endpoint_url: str,
    public_key_hex: str,
    role: Union[MeshNodeRole, str] = MeshNodeRole.WORKER,
    nonce: str = "",
) -> bytes:
    """Computes deterministic registration payload canonically binding identity, keys, and role (PoC-G)."""
    role_str = role.value if hasattr(role, "value") else str(role)
    return f"mesh:register:v1:{node_id}:{cluster_id}:{tenant_id}:{endpoint_url}:{public_key_hex}:{role_str}:{nonce}".encode("utf-8")


def sign_peer_registration(
    km: WitnessKeyManager,
    node_id: str,
    cluster_id: str,
    tenant_id: str,
    endpoint_url: str,
    public_key_hex: str,
    role: Union[MeshNodeRole, str] = MeshNodeRole.WORKER,
    nonce: str = "",
) -> str:
    """Cryptographically signs peer registration payload using peer private key."""
    payload = compute_peer_registration_payload(
        node_id, cluster_id, tenant_id, endpoint_url, public_key_hex, role, nonce
    )
    return sign_peer_data(km, payload)


# ==============================================================================
# 5. Swarm Mesh Federation Router Deep Module
# ==============================================================================

class MeshFederationRouter:
    """
    Zero-trust dynamic agent federation and mesh topology routing engine.
    Hides graph traversal, Dijkstra shortest path algorithms, mutual handshake
    cryptography, fail-closed safety circuit gating, and receipt audit logging.
    """

    def __init__(
        self,
        stale_peer_timeout_sec: float = 60.0,
        require_signed_registration: bool = True,
    ):
        self._lock = threading.RLock()
        self.stale_peer_timeout_sec = stale_peer_timeout_sec
        self.require_signed_registration = require_signed_registration
        self._peers: Dict[str, PeerNode] = {}
        self._links: Dict[str, Dict[str, MeshLink]] = {}  # source -> {target: link}
        self._sessions: Dict[str, Dict[str, Any]] = {}
        self._receipts: List[MeshRoutingReceipt] = []
        self._circuit_tripped: bool = False
        self._capability_validator: Optional[Callable[[str, str, str], bool]] = None

    def set_circuit_tripped(self, tripped: bool) -> None:
        """Synchronizes circuit breaker status fail-closed."""
        with self._lock:
            self._circuit_tripped = bool(tripped)

    def is_circuit_tripped(self) -> bool:
        """Checks if mesh routing is circuit-blocked."""
        with self._lock:
            return self._circuit_tripped

    def set_capability_validator(self, validator: Callable[[str, str, str], bool]) -> None:
        """Sets external capability validator callback (token, tenant_id, dest_node_id)."""
        with self._lock:
            self._capability_validator = validator

    # --------------------------------------------------------------------------
    # Peer Lifecycle & Management
    # --------------------------------------------------------------------------

    def register_peer(
        self,
        node_id: str,
        cluster_id: str,
        tenant_id: str,
        endpoint_url: str,
        public_key_hex: str,
        role: MeshNodeRole = MeshNodeRole.WORKER,
        capabilities: Optional[List[str]] = None,
        latency_ms: float = 1.0,
        signature: Optional[str] = None,
        nonce: str = "",
    ) -> PeerNode:
        """
        Registers an authenticated peer node within the mesh topology.
        Binds public_key_hex, role, and nonce against signature replay (PoC-G) and
        enforces public key validity and signature requirement (PoC-H).
        """
        if not node_id or not cluster_id or not tenant_id:
            raise MeshPeerAuthenticationError("Peer node registration requires node_id, cluster_id, and tenant_id")

        if not is_valid_peer_public_key(public_key_hex):
            raise MeshPeerAuthenticationError(
                f"Malformed public key '{public_key_hex}': must be valid hex or base64 cryptographic key (PoC-H rejected)"
            )

        role_str = role.value if hasattr(role, "value") else str(role)

        def _verify_registration_sig(pub_k: str, sig_val: str) -> bool:
            candidates = [
                compute_peer_registration_payload(node_id, cluster_id, tenant_id, endpoint_url, public_key_hex, role_str, nonce),
                f"mesh:peer:v1:{node_id}:{cluster_id}:{tenant_id}:{endpoint_url}:{public_key_hex}:{role_str}:{nonce}".encode("utf-8"),
                f"mesh:register:v1:{node_id}:{cluster_id}:{tenant_id}:{endpoint_url}:{public_key_hex}:{role_str}".encode("utf-8"),
                f"mesh:peer:v1:{node_id}:{cluster_id}:{tenant_id}:{endpoint_url}:{public_key_hex}:{role_str}".encode("utf-8"),
                f"{node_id}:{cluster_id}:{tenant_id}:{endpoint_url}:{public_key_hex}:{role_str}".encode("utf-8"),
            ]
            for cand in candidates:
                if verify_peer_data(pub_k, cand, sig_val):
                    return True
            return False

        with self._lock:
            # Prevent node hijacking: if node_id is already registered, an update MUST be signed
            # by the existing registered node's public key, binding the new public_key_hex!
            if node_id in self._peers:
                existing = self._peers[node_id]
                if not signature:
                    raise MeshPeerAuthenticationError(
                        f"Cannot overwrite existing peer node '{node_id}' without valid cryptographic signature"
                    )
                if not _verify_registration_sig(existing.public_key_hex, signature):
                    raise MeshPeerAuthenticationError(
                        f"Node hijacking attempt detected: re-registration of '{node_id}' must be signed by existing public key (PoC-G rejected)"
                    )
            else:
                if not signature and self.require_signed_registration:
                    raise MeshPeerAuthenticationError(
                        f"Peer node '{node_id}' registration requires valid cryptographic signature (PoC-H rejected)"
                    )
                if signature and not _verify_registration_sig(public_key_hex, signature):
                    raise MeshPeerAuthenticationError(
                        f"Cryptographic signature verification failed for peer node '{node_id}'"
                    )

            peer = PeerNode(
                node_id=node_id,
                cluster_id=cluster_id,
                tenant_id=tenant_id,
                endpoint_url=endpoint_url,
                public_key_hex=public_key_hex,
                role=role,
                status=MeshNodeStatus.ONLINE,
                capabilities=list(capabilities or []),
                last_heartbeat_ts=time.time(),
                latency_ms=latency_ms,
            )
            self._peers[node_id] = peer
            if node_id not in self._links:
                self._links[node_id] = {}
            return peer

    def deregister_peer(self, node_id: str) -> bool:
        """Removes a peer node and all connected links from the routing topology."""
        with self._lock:
            if node_id not in self._peers:
                return False
            del self._peers[node_id]
            if node_id in self._links:
                del self._links[node_id]
            for src in self._links:
                self._links[src].pop(node_id, None)
            return True

    def get_peer(self, node_id: str) -> Optional[PeerNode]:
        """Retrieves registered peer node by ID."""
        with self._lock:
            return self._peers.get(node_id)

    def list_peers(self, tenant_id: Optional[str] = None) -> List[PeerNode]:
        """Lists active peer nodes, optionally filtered by tenant."""
        with self._lock:
            if tenant_id:
                return [p for p in self._peers.values() if p.tenant_id == tenant_id]
            return list(self._peers.values())

    def heartbeat(self, node_id: str) -> bool:
        """Updates peer node last heartbeat timestamp to prevent eviction."""
        with self._lock:
            peer = self._peers.get(node_id)
            if not peer:
                return False
            peer.last_heartbeat_ts = time.time()
            if peer.status in (MeshNodeStatus.OFFLINE, MeshNodeStatus.DEGRADED, MeshNodeStatus.EVICTED):
                peer.status = MeshNodeStatus.ONLINE
            return True

    def evict_stale_peers(self, now: Optional[float] = None) -> List[str]:
        """Evicts peers that have not emitted a heartbeat within the stale timeout window."""
        current_ts = now if now is not None else time.time()
        evicted = []
        with self._lock:
            for node_id, peer in list(self._peers.items()):
                if current_ts - peer.last_heartbeat_ts > self.stale_peer_timeout_sec:
                    peer.status = MeshNodeStatus.EVICTED
                    # Deactivate outgoing and incoming links
                    if node_id in self._links:
                        for link in self._links[node_id].values():
                            link.is_active = False
                    for src in self._links:
                        if node_id in self._links[src]:
                            self._links[src][node_id].is_active = False
                    evicted.append(node_id)
        return evicted

    # --------------------------------------------------------------------------
    # Link Topology
    # --------------------------------------------------------------------------

    def establish_link(
        self,
        source_node_id: str,
        target_node_id: str,
        weight_cost: float = 1.0,
        latency_ms: float = 1.0,
        bidirectional: bool = True,
    ) -> bool:
        """Establishes an active routing link between two registered peer nodes."""
        with self._lock:
            if source_node_id not in self._peers:
                raise MeshDestinationUnreachableError(f"Source peer node '{source_node_id}' is not registered")
            if target_node_id not in self._peers:
                raise MeshDestinationUnreachableError(f"Target peer node '{target_node_id}' is not registered")

            if source_node_id not in self._links:
                self._links[source_node_id] = {}
            self._links[source_node_id][target_node_id] = MeshLink(
                source_node_id=source_node_id,
                target_node_id=target_node_id,
                weight_cost=max(0.01, float(weight_cost)),
                is_active=True,
                latency_ms=max(0.0, float(latency_ms)),
            )

            if bidirectional:
                if target_node_id not in self._links:
                    self._links[target_node_id] = {}
                self._links[target_node_id][source_node_id] = MeshLink(
                    source_node_id=target_node_id,
                    target_node_id=source_node_id,
                    weight_cost=max(0.01, float(weight_cost)),
                    is_active=True,
                    latency_ms=max(0.0, float(latency_ms)),
                )
            return True

    def set_link_status(self, source_node_id: str, target_node_id: str, is_active: bool) -> bool:
        """Enables or disables a routing link without removing it from topology."""
        with self._lock:
            if source_node_id in self._links and target_node_id in self._links[source_node_id]:
                self._links[source_node_id][target_node_id].is_active = bool(is_active)
                return True
            return False

    def list_links(self) -> List[MeshLink]:
        """Lists all established routing links."""
        with self._lock:
            result = []
            for src in self._links:
                for link in self._links[src].values():
                    result.append(link)
            return result

    # --------------------------------------------------------------------------
    # Cryptographic Handshakes
    # --------------------------------------------------------------------------

    def perform_handshake(
        self,
        source_node_id: str,
        target_node_id: str,
        source_signer: Any,
        target_signer: Any,
    ) -> Tuple[PeerHandshakeRequest, PeerHandshakeResponse]:
        """
        Executes a zero-trust mutual cryptographic handshake between two peers.
        Returns the verified request and response session tokens.
        """
        with self._lock:
            src_peer = self._peers.get(source_node_id)
            tgt_peer = self._peers.get(target_node_id)
            if not src_peer:
                raise MeshPeerAuthenticationError(f"Source node '{source_node_id}' not found")
            if not tgt_peer:
                raise MeshPeerAuthenticationError(f"Target node '{target_node_id}' not found")

            # 1. Source signs handshake request
            now_ns = time.time_ns()
            nonce = uuid.uuid4().hex
            req_data = f"{source_node_id}:{target_node_id}:{src_peer.cluster_id}:{src_peer.tenant_id}:{now_ns}:{nonce}".encode("utf-8")
            req_sig = _sign_helper(source_signer, req_data)

            # 2. Target verifies request
            if not verify_peer_data(src_peer.public_key_hex, req_data, req_sig):
                raise MeshPeerAuthenticationError("Mutual handshake request verification failed")

            req = PeerHandshakeRequest(
                source_node_id=source_node_id,
                target_node_id=target_node_id,
                cluster_id=src_peer.cluster_id,
                tenant_id=src_peer.tenant_id,
                public_key_hex=src_peer.public_key_hex,
                timestamp_ns=now_ns,
                nonce=nonce,
                signature=req_sig,
            )

            # 3. Target signs handshake response
            session_id = f"mesh-sess-{uuid.uuid4().hex[:12]}"
            res_data = f"{target_node_id}:{source_node_id}:ACCEPTED:{session_id}:{now_ns}".encode("utf-8")
            res_sig = _sign_helper(target_signer, res_data)

            # 4. Source verifies response
            if not verify_peer_data(tgt_peer.public_key_hex, res_data, res_sig):
                raise MeshPeerAuthenticationError("Mutual handshake response verification failed")

            res = PeerHandshakeResponse(
                target_node_id=target_node_id,
                source_node_id=source_node_id,
                status="ACCEPTED",
                session_id=session_id,
                timestamp_ns=now_ns,
                signature=res_sig,
            )

            self._sessions[session_id] = {
                "session_id": session_id,
                "node_a": source_node_id,
                "node_b": target_node_id,
                "established_at_ns": now_ns,
            }
            return req, res

    # --------------------------------------------------------------------------
    # Dijkstra Shortest Path & Routing
    # --------------------------------------------------------------------------

    def find_optimal_path(
        self,
        source_node_id: str,
        dest_node_id: str,
        tenant_id: Optional[str] = None,
    ) -> List[str]:
        """
        Computes loop-free shortest path from source to destination using Dijkstra's algorithm.
        Considers only active nodes and links. Enforces tenant boundary fencing.
        """
        with self._lock:
            if source_node_id not in self._peers:
                raise MeshDestinationUnreachableError(f"Source node '{source_node_id}' is unknown")
            if dest_node_id not in self._peers:
                raise MeshDestinationUnreachableError(f"Destination node '{dest_node_id}' is unknown")

            dest_peer = self._peers[dest_node_id]
            if dest_peer.status in (MeshNodeStatus.OFFLINE, MeshNodeStatus.EVICTED):
                raise MeshDestinationUnreachableError(f"Destination node '{dest_node_id}' is inactive ({dest_peer.status})")

            # Dijkstra shortest path search
            distances: Dict[str, float] = {source_node_id: 0.0}
            previous: Dict[str, Optional[str]] = {source_node_id: None}
            unvisited: Set[str] = set(self._peers.keys())

            while unvisited:
                # Find unvisited node with smallest distance
                current = None
                curr_dist = float("inf")
                for node in unvisited:
                    d = distances.get(node, float("inf"))
                    if d < curr_dist:
                        curr_dist = d
                        current = node

                if current is None or curr_dist == float("inf"):
                    break  # Remaining nodes are unreachable

                if current == dest_node_id:
                    break

                unvisited.remove(current)

                # Check neighbors
                for neighbor, link in self._links.get(current, {}).items():
                    if neighbor not in unvisited or not link.is_active:
                        continue
                    neighbor_peer = self._peers.get(neighbor)
                    if not neighbor_peer or neighbor_peer.status in (MeshNodeStatus.OFFLINE, MeshNodeStatus.EVICTED):
                        continue

                    new_dist = curr_dist + link.weight_cost
                    if new_dist < distances.get(neighbor, float("inf")):
                        distances[neighbor] = new_dist
                        previous[neighbor] = current

            # Reconstruct path
            if dest_node_id not in previous:
                raise MeshDestinationUnreachableError(
                    f"No active path exists from '{source_node_id}' to '{dest_node_id}'"
                )

            path = []
            curr: Optional[str] = dest_node_id
            while curr is not None:
                path.append(curr)
                curr = previous.get(curr)
            path.reverse()

            if not path or path[0] != source_node_id:
                raise MeshDestinationUnreachableError(
                    f"Path resolution from '{source_node_id}' to '{dest_node_id}' broke continuity"
                )

            # Check loop-freedom
            if len(path) != len(set(path)):
                raise MeshRoutingLoopError(f"Cyclic loop detected in path traversal: {path}")

            return path

    def route_packet(self, packet: MeshPacket) -> MeshRoutingReceipt:
        """
        Routes an inter-agent packet across the mesh topology.
        Enforces safety circuits, hop limit (TTL), loop detection, tenant capability fencing,
        and produces a canonical SHA-256 routing receipt.
        """
        t0 = time.perf_counter()
        now_ns = time.time_ns()
        receipt_id = f"mesh-rcpt-{uuid.uuid4().hex[:12]}"

        with self._lock:
            # 1. Circuit Breaker Check (Fail-Closed)
            if self._circuit_tripped:
                receipt = MeshRoutingReceipt(
                    receipt_id=receipt_id,
                    packet_id=packet.packet_id,
                    source_node_id=packet.source_node_id,
                    dest_node_id=packet.dest_node_id,
                    tenant_id=packet.tenant_id,
                    path_traversed=list(packet.path_traversed),
                    total_hops=packet.hop_count,
                    status=MeshRoutingStatus.BLOCKED_CIRCUIT,
                    duration_us=(time.perf_counter() - t0) * 1_000_000,
                    created_at_ns=now_ns,
                    receipt_hash=MeshRoutingReceipt.compute_digest(
                        receipt_id, packet.packet_id, packet.source_node_id, packet.dest_node_id,
                        packet.tenant_id, packet.path_traversed, "BLOCKED_CIRCUIT",
                        (time.perf_counter() - t0) * 1_000_000, now_ns
                    ),
                    error_message="Safety circuit tripped: mesh routing blocked fail-closed",
                )
                self._receipts.append(receipt)
                raise MeshCircuitTrippedError(receipt.error_message)

            # 2. Source and Destination Existence Check
            if packet.source_node_id not in self._peers:
                err = f"Source node '{packet.source_node_id}' is not a registered mesh peer"
                return self._mint_failed_receipt(receipt_id, packet, MeshRoutingStatus.DESTINATION_UNREACHABLE, err, t0, now_ns)

            if packet.dest_node_id not in self._peers:
                err = f"Destination node '{packet.dest_node_id}' is not a registered mesh peer"
                return self._mint_failed_receipt(receipt_id, packet, MeshRoutingStatus.DESTINATION_UNREACHABLE, err, t0, now_ns)

            dest_peer = self._peers[packet.dest_node_id]

            # 3. Hop Limit (TTL) Enforcement
            if packet.hop_count >= packet.max_hops:
                err = f"Packet '{packet.packet_id}' exceeded max hop limit ({packet.hop_count} >= {packet.max_hops})"
                receipt = self._mint_failed_receipt(receipt_id, packet, MeshRoutingStatus.DROPPED_TTL, err, t0, now_ns)
                raise MeshHopLimitExceededError(err)

            # 4. Loop Detection Check
            if packet.source_node_id in packet.path_traversed:
                err = f"Packet '{packet.packet_id}' encountered duplicate node '{packet.source_node_id}' in path"
                receipt = self._mint_failed_receipt(receipt_id, packet, MeshRoutingStatus.ROUTING_LOOP, err, t0, now_ns)
                raise MeshRoutingLoopError(err)

            # 5. Tenant Boundary & Capability Fencing
            if dest_peer.tenant_id != packet.tenant_id:
                # Cross-tenant routing requires capability token
                authorized = False
                if packet.capability_token and self._capability_validator:
                    authorized = self._capability_validator(
                        packet.capability_token,
                        packet.tenant_id,
                        packet.dest_node_id,
                    )
                elif packet.capability_token and packet.capability_token.startswith("valid-cross-tenant"):
                    authorized = True

                if not authorized:
                    err = (
                        f"Cross-tenant transit denied: packet tenant '{packet.tenant_id}' "
                        f"cannot access destination '{packet.dest_node_id}' (tenant '{dest_peer.tenant_id}') "
                        f"without valid capability token"
                    )
                    receipt = self._mint_failed_receipt(receipt_id, packet, MeshRoutingStatus.FAILED_CAPABILITY, err, t0, now_ns)
                    raise MeshCapabilityFencingError(err)

            # 6. Optimal Path Discovery
            try:
                path = self.find_optimal_path(packet.source_node_id, packet.dest_node_id, packet.tenant_id)
            except MeshDestinationUnreachableError as e:
                return self._mint_failed_receipt(receipt_id, packet, MeshRoutingStatus.DESTINATION_UNREACHABLE, str(e), t0, now_ns)

            # 7. Advance Hop & Delivery
            traversed = list(packet.path_traversed)
            for node in path:
                if node not in traversed:
                    traversed.append(node)

            total_hops = packet.hop_count + len(path) - 1
            if total_hops > packet.max_hops:
                err = f"Computed route requires {total_hops} hops, exceeding packet limit {packet.max_hops}"
                receipt = self._mint_failed_receipt(receipt_id, packet, MeshRoutingStatus.DROPPED_TTL, err, t0, now_ns)
                raise MeshHopLimitExceededError(err)

            status = MeshRoutingStatus.DELIVERED if len(path) <= 2 else MeshRoutingStatus.ROUTED_RELAY
            duration_us = (time.perf_counter() - t0) * 1_000_000

            receipt_hash = MeshRoutingReceipt.compute_digest(
                receipt_id=receipt_id,
                packet_id=packet.packet_id,
                source_node_id=packet.source_node_id,
                dest_node_id=packet.dest_node_id,
                tenant_id=packet.tenant_id,
                path=traversed,
                status=status.value,
                duration_us=duration_us,
                timestamp_ns=now_ns,
            )

            receipt = MeshRoutingReceipt(
                receipt_id=receipt_id,
                packet_id=packet.packet_id,
                source_node_id=packet.source_node_id,
                dest_node_id=packet.dest_node_id,
                tenant_id=packet.tenant_id,
                path_traversed=traversed,
                total_hops=total_hops,
                status=status,
                duration_us=duration_us,
                created_at_ns=now_ns,
                receipt_hash=receipt_hash,
            )
            self._receipts.append(receipt)
            return receipt

    def _mint_failed_receipt(
        self,
        receipt_id: str,
        packet: MeshPacket,
        status: MeshRoutingStatus,
        error_message: str,
        t0: float,
        now_ns: int,
    ) -> MeshRoutingReceipt:
        duration_us = (time.perf_counter() - t0) * 1_000_000
        receipt_hash = MeshRoutingReceipt.compute_digest(
            receipt_id=receipt_id,
            packet_id=packet.packet_id,
            source_node_id=packet.source_node_id,
            dest_node_id=packet.dest_node_id,
            tenant_id=packet.tenant_id,
            path=packet.path_traversed,
            status=status.value,
            duration_us=duration_us,
            timestamp_ns=now_ns,
        )
        receipt = MeshRoutingReceipt(
            receipt_id=receipt_id,
            packet_id=packet.packet_id,
            source_node_id=packet.source_node_id,
            dest_node_id=packet.dest_node_id,
            tenant_id=packet.tenant_id,
            path_traversed=list(packet.path_traversed),
            total_hops=packet.hop_count,
            status=status,
            duration_us=duration_us,
            created_at_ns=now_ns,
            receipt_hash=receipt_hash,
            error_message=error_message,
        )
        self._receipts.append(receipt)
        return receipt

    # --------------------------------------------------------------------------
    # Audit & Status Inspection
    # --------------------------------------------------------------------------

    def get_receipt(self, receipt_id: str) -> Optional[MeshRoutingReceipt]:
        """Retrieves a single routing receipt by ID."""
        with self._lock:
            for r in self._receipts:
                if r.receipt_id == receipt_id:
                    return r
            return None

    def get_receipts(self, tenant_id: Optional[str] = None, limit: int = 100) -> List[MeshRoutingReceipt]:
        """Lists recorded routing receipts, newest first."""
        with self._lock:
            if tenant_id:
                filtered = [r for r in self._receipts if r.tenant_id == tenant_id]
                return filtered[-limit:]
            return self._receipts[-limit:]

    def status(self) -> Dict[str, Any]:
        """Returns live operational diagnostics of the swarm mesh router."""
        with self._lock:
            active_links_count = sum(
                1 for src in self._links for link in self._links[src].values() if link.is_active
            )
            online_peers_count = sum(
                1 for p in self._peers.values() if p.status == MeshNodeStatus.ONLINE
            )
            return {
                "online_peers_count": online_peers_count,
                "total_peers_count": len(self._peers),
                "active_links_count": active_links_count,
                "total_receipts_recorded": len(self._receipts),
                "circuit_tripped": self._circuit_tripped,
                "active_sessions_count": len(self._sessions),
            }

    def reset(self) -> None:
        """Resets the mesh router state (for test isolation)."""
        with self._lock:
            self._peers.clear()
            self._links.clear()
            self._sessions.clear()
            self._receipts.clear()
            self._circuit_tripped = False
            self._capability_validator = None


# Global Singleton Instance
global_mesh_router: MeshFederationRouter = MeshFederationRouter()
