# -*- coding: utf-8 -*-
"""
SwarmWatch Official Framework Ecosystem Adapters.
Seamless plug-and-play monitoring, circuit breakers, and time-machine debugging
for LangGraph, CrewAI, AutoGen, and custom multi-agent runtimes.
"""

from swarmwatch.adapters.langgraph import SwarmWatchCallbackHandler, patch_graph
from swarmwatch.adapters.crewai import SwarmWatchCrewListener
from swarmwatch.adapters.llamaindex import SwarmWatchLlamaIndexHandler
from swarmwatch.adapters.autogen import SwarmWatchAutoGenListener
from swarmwatch.adapters.dspy import SwarmWatchDSPyTracker
from swarmwatch.adapters.semantic_kernel import (
    SwarmWatchSemanticKernelFilter,
    FunctionInvocationContext,
)

__all__ = [
    "SwarmWatchCallbackHandler",
    "patch_graph",
    "SwarmWatchCrewListener",
    "SwarmWatchLlamaIndexHandler",
    "SwarmWatchAutoGenListener",
    "SwarmWatchDSPyTracker",
    "SwarmWatchSemanticKernelFilter",
    "FunctionInvocationContext",
]

