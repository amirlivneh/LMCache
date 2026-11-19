# SPDX-License-Identifier: Apache-2.0
"""Request teardown discards results published before and after END_SESSION."""

# Standard
from threading import Event
import time

# Third Party
import pytest
import torch

# First Party
from lmcache.v1.common.api import MemoryLayoutDesc
from lmcache.v1.common.custom_types import IPCCacheServerKey
from lmcache.v1.distributed.api import ObjectKey
from lmcache.v1.distributed.config import (
    EvictionConfig,
    L1ManagerConfig,
    L1MemoryManagerConfig,
    StorageManagerConfig,
)
from lmcache.v1.distributed.l2_adapters.base import L2TaskId
from lmcache.v1.distributed.l2_adapters.config import L2AdaptersConfig
from lmcache.v1.distributed.l2_adapters.mock_l2_adapter import MockL2AdapterConfig
from lmcache.v1.multiprocess.engine_context import MPCacheServerContext
from lmcache.v1.multiprocess.modules.lookup import LookupModule

pytestmark = pytest.mark.no_shared_allocator


@pytest.mark.parametrize("wait_for_l2", [False, True], ids=["inline", "in-flight"])
def test_end_session_discards_unpolled_prefetch(
    monkeypatch: pytest.MonkeyPatch, wait_for_l2: bool
) -> None:
    """End a request without polling and let pending L2 work settle."""
    monkeypatch.setattr(
        "lmcache.v1.memory_management._allocate_cpu_memory",
        lambda size, *args, **kwargs: torch.empty(size, dtype=torch.uint8),
    )
    monkeypatch.setattr(
        "lmcache.v1.memory_management._free_cpu_memory", lambda *args, **kwargs: None
    )
    config = StorageManagerConfig(
        l1_manager_config=L1ManagerConfig(
            L1MemoryManagerConfig(size_in_bytes=4096, use_lazy=False, shm_name="")
        ),
        eviction_config=EvictionConfig("noop"),
        l2_adapter_config=L2AdaptersConfig(
            adapters=[MockL2AdapterConfig(0.01, 1.0)] if wait_for_l2 else []
        ),
    )
    ctx = MPCacheServerContext(config, chunk_size=16)
    lookup_entered = Event()
    allow_lookup = Event()
    try:
        storage = ctx.storage_manager
        if wait_for_l2:
            [(_, adapter)] = storage.l2_adapters()
            submit = adapter.submit_lookup_and_lock_task

            def wait_then_submit(
                keys: list[ObjectKey], layouts: dict[int, MemoryLayoutDesc]
            ) -> L2TaskId:
                lookup_entered.set()
                assert allow_lookup.wait(5)
                return submit(keys, layouts)

            monkeypatch.setattr(
                adapter, "submit_lookup_and_lock_task", wait_then_submit
            )

        module = LookupModule(ctx)
        layout = MemoryLayoutDesc([torch.Size([64])], [torch.uint8])
        ctx.layout_desc_registry.register("model", 1, layout, 16, "blake3")
        key = IPCCacheServerKey.from_token_ids(
            model_name="model",
            world_size=1,
            worker_id=None,
            token_ids=range(16),
            start=0,
            end=16,
            request_id="cancelled",
        )
        module.lookup(key, tp_size=1)
        if wait_for_l2:
            assert lookup_entered.wait(5)
        module.end_session(key.request_id)
        allow_lookup.set()

        deadline = time.monotonic() + 5
        while True:
            prefetch = storage.report_status()["prefetch_controller"]
            if (
                not prefetch["in_flight_request_count"]
                and not prefetch["submission_queue_size"]
                and not prefetch["pending_queue_size"]
            ):
                break
            assert time.monotonic() < deadline, "prefetch did not finish"
            time.sleep(0.005)
        assert ctx.session_manager.get(key.request_id) is None
        assert (
            module.report_status()["active_prefetch_jobs"],
            prefetch["completed_results_count"],
        ) == (0, 0)
        assert module.query_prefetch_status(key.request_id) == 0
    finally:
        allow_lookup.set()
        ctx.close()
