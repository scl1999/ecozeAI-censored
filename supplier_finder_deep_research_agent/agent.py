import logging
import os

# Configure logging immediately
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Enable telemetry for Cloud Trace visibility in Agent Engine dashboard
os.environ["GOOGLE_CLOUD_AGENT_ENGINE_ENABLE_TELEMETRY"] = "true"
# Do not disable OTEL SDK - needed for --trace_to_cloud to work
# If SSL errors return, we'll handle them differently

os.environ.setdefault("GOOGLE_CLOUD_PROJECT", "...")
os.environ.setdefault("GOOGLE_CLOUD_LOCATION", "global")
os.environ.setdefault("GOOGLE_GENAI_USE_VERTEXAI", "True")
# Set location to global to support gemini-3-pro-preview via global endpoint
# os.environ["GOOGLE_CLOUD_LOCATION"] = "global"

from functools import cached_property
from google.genai import Client
from google.adk.agents import LlmAgent
from google.adk.tools import agent_tool
from google.genai.types import ThinkingConfig, GenerateContentConfig, HttpOptions
from google.adk.planners import BuiltInPlanner
from google.adk.models.google_llm import Gemini

# Import built-in tools
from google.adk.tools.google_search_tool import GoogleSearchTool
from google.adk.tools import url_context


class GlobalGemini(Gemini):
    @cached_property
    def api_client(self) -> Client:
        http_options_kwargs = {"headers": self._tracking_headers}
        if self.retry_options:
            http_options_kwargs["retry_options"] = self.retry_options

        return Client(
            location="global", http_options=HttpOptions(**http_options_kwargs)
        )


# System instructions from apcfSupplierFinder (SYS_APCFSF)
SYS_APCFSF = """...
"""

fact_finder_google_search_agent = LlmAgent(
    name="fact_finder_google_search_agent",
    model=GlobalGemini(model="gemini-3-flash-preview"),
    description=("Agent specialized in performing Google searches."),
    sub_agents=[],
    instruction="Use the GoogleSearchTool to find information on the web.",
    tools=[GoogleSearchTool()],
    generate_content_config=GenerateContentConfig(
        temperature=1.0, max_output_tokens=65535
    ),
    planner=BuiltInPlanner(
        thinking_config=ThinkingConfig(include_thoughts=True, thinking_level="HIGH")
    ),
)
fact_finder_url_context_agent = LlmAgent(
    name="fact_finder_url_context_agent",
    model="gemini-2.5-flash",
    description=("Agent specialized in fetching content from URLs."),
    sub_agents=[],
    instruction="Use the UrlContextTool to retrieve content from provided URLs.",
    tools=[url_context],
    generate_content_config=GenerateContentConfig(
        temperature=1.0, max_output_tokens=65535
    ),
    planner=BuiltInPlanner(
        thinking_config=ThinkingConfig(include_thoughts=True, thinking_budget=24576)
    ),
)
fact_finder = LlmAgent(
    name="fact_finder",
    model=GlobalGemini(model="gemini-3-pro-preview"),
    description=(
        "Agent that handles a specific task, give it the exact prompt given to you by the user."
    ),
    sub_agents=[],
    instruction='...',
    tools=[
        agent_tool.AgentTool(agent=fact_finder_google_search_agent),
        agent_tool.AgentTool(agent=fact_finder_url_context_agent),
    ],
    generate_content_config=GenerateContentConfig(
        temperature=1.0, max_output_tokens=65535
    ),
    planner=BuiltInPlanner(
        thinking_config=ThinkingConfig(include_thoughts=True, thinking_level="HIGH")
    ),
)
fact_checker_google_search_agent = LlmAgent(
    name="fact_checker_google_search_agent",
    model=GlobalGemini(model="gemini-3-flash-preview"),
    description=("Agent specialized in performing Google searches."),
    sub_agents=[],
    instruction="Use the GoogleSearchTool to find information on the web.",
    tools=[GoogleSearchTool()],
    generate_content_config=GenerateContentConfig(
        temperature=1.0, max_output_tokens=65535
    ),
    planner=BuiltInPlanner(
        thinking_config=ThinkingConfig(include_thoughts=True, thinking_level="HIGH")
    ),
)
fact_checker_url_context_agent = LlmAgent(
    name="fact_checker_url_context_agent",
    model="gemini-2.5-flash",
    description=("Agent specialized in fetching content from URLs."),
    sub_agents=[],
    instruction="Use the UrlContextTool to retrieve content from provided URLs.",
    tools=[url_context],
    generate_content_config=GenerateContentConfig(
        temperature=1.0, max_output_tokens=65535
    ),
    planner=BuiltInPlanner(
        thinking_config=ThinkingConfig(include_thoughts=True, thinking_budget=24576)
    ),
)
fact_checker_2 = LlmAgent(
    name="fact_checker_2",
    model=GlobalGemini(model="gemini-3-flash-preview"),
    description=("Fact checks the fact_finder AI agent. "),
    sub_agents=[],
    instruction='...',
    tools=[
        agent_tool.AgentTool(agent=fact_checker_google_search_agent),
        agent_tool.AgentTool(agent=fact_checker_url_context_agent),
    ],
    generate_content_config=GenerateContentConfig(
        temperature=1.0, max_output_tokens=65535
    ),
    planner=BuiltInPlanner(
        thinking_config=ThinkingConfig(include_thoughts=True, thinking_level="HIGH")
    ),
)

# Configure the root agent with specified parameters
root_agent = LlmAgent(
    name="supplier_finder",
    model=GlobalGemini(model="gemini-3-flash-preview"),
    description="Main Agent orchestrator for finding a supplier for a product / component / material",
    sub_agents=[fact_finder, fact_checker_2],
    instruction=SYS_APCFSF,
    tools=[],
    generate_content_config=GenerateContentConfig(
        temperature=1.0, max_output_tokens=65535
    ),
    planner=BuiltInPlanner(
        thinking_config=ThinkingConfig(include_thoughts=True, thinking_level="HIGH")
    ),
)

from google.adk.apps.app import App, ResumabilityConfig

# Use basic App wrapper - let Agent Engine use its default VertexAiSessionService
# Cloud Function will NOT pass session_id, letting each request be a new session
app = App(
    name="supplier_finder",
    root_agent=root_agent,
    # Set the resumability config to enable resumability.
    resumability_config=ResumabilityConfig(
        is_resumable=True,
    ),
)
